"""
Five canonical INC monitoring queries, each implemented (where possible)
in Oracle, Sonata-style, and UnivMon-style systems.

  Q1: Per-round completion latency.        Oracle | Sonata | -UnivMon-
  Q2: Per-worker contribution count.       Oracle | Sonata | UnivMon (degraded)
  Q3: Straggler identification.            Oracle | Sonata | -UnivMon-
  Q4: Round failure detection.             Oracle | Sonata (awkward) | -UnivMon-
  Q5: Cross-job interference.              Oracle | -Sonata- | -UnivMon-

Inexpressibility is marked with hyphens. A function returning
`NotExpressible` means we have judged the query impossible to write in
that system's native primitives and explain why in the comment.
"""

from collections import defaultdict
from typing import Dict, List, Tuple, Any

from systems import (
    Oracle, Pipeline, Filter, Map, ReduceByKey, Distinct, UnivMonSketch,
)


class NotExpressible(Exception):
    """Marker for queries that cannot be expressed in a given system."""
    pass


# =============================================================================
# Q1: Per-round completion latency
# =============================================================================
# For each (job, round), measure time from first packet seen to last packet
# seen. The fundamental monitoring metric for round-based INC.
# =============================================================================

def q1_oracle(oracle: Oracle) -> Dict[Tuple[int, int], float]:
    state: Dict[Tuple[int, int], List[float]] = {}
    for p in oracle.packets:
        k = (p.job_id, p.round_id)
        if k not in state:
            state[k] = [p.timestamp, p.timestamp]
        else:
            if p.timestamp < state[k][0]:
                state[k][0] = p.timestamp
            if p.timestamp > state[k][1]:
                state[k][1] = p.timestamp
    return {k: v[1] - v[0] for k, v in state.items()}


def q1_sonata(packets) -> Tuple[Dict[Tuple[int, int], float], int]:
    """Expressible: reduce_by_key over (job, round) tracking (min_t, max_t).
    Latency = max_t - min_t. State entries = number of (job, round) tuples.
    """
    pipeline = Pipeline(
        Map(lambda p: ((p.job_id, p.round_id), p.timestamp)),
        ReduceByKey(
            key_fn=lambda kv: kv[0],
            value_fn=lambda kv: (kv[1], kv[1]),
            agg_fn=lambda a, b: (min(a[0], b[0]), max(a[1], b[1])),
            initial=(float("inf"), float("-inf")),
        ),
    )
    result = pipeline.run(packets)
    latencies = {k: v[1] - v[0] for k, v in result}
    return latencies, pipeline.total_state


def q1_univmon(packets) -> None:
    """NOT EXPRESSIBLE.

    A Count-Sketch over any flow key returns frequency counts, not
    timestamps. UnivMon's universal sketch supports G-sums of frequencies
    (heavy hitters, entropy, distinct counts) but has no primitive for
    tracking min/max of an attached scalar like a timestamp. The query
    sits outside the sketch abstraction entirely.
    """
    raise NotExpressible(
        "Sketches estimate frequency distributions over flow keys; they "
        "do not retain per-key timestamp extrema."
    )


# =============================================================================
# Q2: Per-worker contribution count per round
# =============================================================================
# For each (job, round, worker), count packets contributed. Detects
# asymmetric participation and packet loss when one worker's count falls
# short of its peers.
# =============================================================================

def q2_oracle(oracle: Oracle) -> Dict[Tuple[int, int, int], int]:
    counts: Dict[Tuple[int, int, int], int] = defaultdict(int)
    for p in oracle.packets:
        counts[(p.job_id, p.round_id, p.worker_id)] += 1
    return dict(counts)


def q2_sonata(packets) -> Tuple[Dict[Tuple[int, int, int], int], int]:
    """Expressible: reduce_by_key with sum. The canonical fit for Sonata."""
    pipeline = Pipeline(
        Map(lambda p: ((p.job_id, p.round_id, p.worker_id), 1)),
        ReduceByKey(
            key_fn=lambda kv: kv[0],
            value_fn=lambda kv: kv[1],
            agg_fn=lambda a, b: a + b,
            initial=0,
        ),
    )
    result = pipeline.run(packets)
    return dict(result), pipeline.total_state


def q2_sonata_from_counts(
    counts: Dict[Tuple[int, int, int], int]
) -> Tuple[Dict[Tuple[int, int, int], int], int]:
    """Equivalent Q2 result when the workload is already count-compressed."""
    return dict(counts), len(counts)


def q2_univmon(
    packets, width: int = 1024, depth: int = 5
) -> Tuple[Dict[Tuple[int, int, int], int], int]:
    """Partially expressible.

    A sketch keyed on (job, round, worker) gives approximate counts per
    key. But the sketch abstraction returns *heavy hitters* given a
    threshold; it does not enumerate the universe of keys, and it cannot
    distinguish ``the lossy worker'' from any other worker when all
    workers are equally heavy by design. We let the sketch cheat by
    iterating over observed_keys; even with this concession, the
    estimator's variance makes per-key counts noisy.
    """
    sketch = UnivMonSketch(
        levels=1, width=width, depth=depth,
        key_fn=lambda p: (p.job_id, p.round_id, p.worker_id),
    )
    sketch.ingest(packets)
    counts = {
        k: max(0, sketch.estimate(k))
        for k in sketch.sketches[0].observed_keys
    }
    return counts, sketch.state_entries


def q2_univmon_from_counts(
    counts: Dict[Tuple[int, int, int], int],
    width: int = 1024,
    depth: int = 5,
) -> Tuple[Dict[Tuple[int, int, int], int], int]:
    """Sketch Q2 from count-compressed input.

    Updating a sketch key by its aggregate count is equivalent to applying
    that many unit updates, but it avoids paper-scale packet materialization.
    """
    sketch = UnivMonSketch(
        levels=1, width=width, depth=depth,
        key_fn=lambda key_count: key_count[0],
    )
    for key, count in counts.items():
        sketch.sketches[0].update(key, count)
    estimated = {
        k: max(0, sketch.estimate(k))
        for k in sketch.sketches[0].observed_keys
    }
    return estimated, sketch.state_entries


def q2_univmon_from_counts_keyed(
    counts: Dict[Tuple[int, int, int], int],
    key_mode: str,
    width: int = 1024,
    depth: int = 5,
) -> Tuple[Dict[Tuple[int, int, int], int], int]:
    """Sketch Q2 under different UnivMon flow-key choices.

    Coarser keys are projected back to per-round counts by evenly
    distributing the aggregate estimate across the original keys they
    collapsed. That models the information loss: once round_id is omitted
    from the sketch key, the sketch cannot recover which round was bad.
    """
    if key_mode == "worker":
        project = lambda key: (key[2],)
    elif key_mode == "job_worker":
        project = lambda key: (key[0], key[2])
    elif key_mode == "job_round_worker":
        project = lambda key: key
    elif key_mode == "job_round_worker_chunk":
        raise NotExpressible(
            "A chunk-level sketch key requires enumerating the expected "
            "(job, round, worker, chunk) universe to reconstruct Q2."
        )
    else:
        raise ValueError(f"unknown UnivMon key mode: {key_mode}")

    sketch = UnivMonSketch(
        levels=1, width=width, depth=depth,
        key_fn=lambda key_count: key_count[0],
    )
    collapsed: Dict[Tuple, int] = defaultdict(int)
    fanout: Dict[Tuple, int] = defaultdict(int)
    for key, count in counts.items():
        projected = project(key)
        collapsed[projected] += count
        fanout[projected] += 1

    for projected, count in collapsed.items():
        sketch.sketches[0].update(projected, count)

    estimated: Dict[Tuple[int, int, int], int] = {}
    for key in counts:
        projected = project(key)
        divisor = max(1, fanout[projected])
        estimated[key] = max(0, round(sketch.estimate(projected) / divisor))
    return estimated, sketch.state_entries


# =============================================================================
# Q3: Straggler identification
# =============================================================================
# For each (job, round), identify the worker whose last packet arrived
# latest. The defining query for tail-behavior monitoring.
# =============================================================================

def q3_oracle(oracle: Oracle) -> Dict[Tuple[int, int], int]:
    last: Dict[Tuple[int, int], Tuple[float, int]] = defaultdict(
        lambda: (float("-inf"), -1)
    )
    for p in oracle.packets:
        k = (p.job_id, p.round_id)
        if p.timestamp > last[k][0]:
            last[k] = (p.timestamp, p.worker_id)
    return {k: v[1] for k, v in last.items()}


def q3_sonata(packets) -> Tuple[Dict[Tuple[int, int], int], int]:
    """Expressible but awkward.

    Sonata's reduce_by_key supports arbitrary user-supplied reducers, so
    we can carry a (max_timestamp, argmax_worker) tuple through the
    reduction. This works but stretches the abstraction: dataflow
    operators in Sonata's published examples carry simple scalars
    (counts, sums), not (scalar, identity) pairs. We mark this as YELLOW
    in the expressiveness table.
    """
    pipeline = Pipeline(
        Map(lambda p: ((p.job_id, p.round_id), (p.timestamp, p.worker_id))),
        ReduceByKey(
            key_fn=lambda kv: kv[0],
            value_fn=lambda kv: kv[1],
            agg_fn=lambda a, b: a if a[0] >= b[0] else b,
            initial=(float("-inf"), -1),
        ),
    )
    result = pipeline.run(packets)
    return {k: v[1] for k, v in result}, pipeline.total_state


def q3_univmon(packets) -> None:
    """NOT EXPRESSIBLE.

    Argmax over keys is not a sketch primitive. Heavy-hitter detection
    returns keys with high frequency; here all workers have equal
    frequency by design, so heavy-hitter rank reveals no information
    about which worker is the straggler.
    """
    raise NotExpressible(
        "Argmax over keys is not expressible as a sketch G-sum; all "
        "workers are equally heavy by construction."
    )


# =============================================================================
# Q4: Round failure detection
# =============================================================================
# For each (job, round), did all expected (worker, chunk) pairs arrive?
# Requires comparing the observed set against an externally supplied
# expected cardinality.
# =============================================================================

def q4_oracle(
    oracle: Oracle, expected_workers: int, expected_chunks: int
) -> Dict[Tuple[int, int], bool]:
    seen: Dict[Tuple[int, int], set] = defaultdict(set)
    for p in oracle.packets:
        seen[(p.job_id, p.round_id)].add((p.worker_id, p.chunk_id))
    expected = expected_workers * expected_chunks
    return {k: len(v) < expected for k, v in seen.items()}


def q4_sonata(
    packets, expected_workers: int, expected_chunks: int
) -> Tuple[Dict[Tuple[int, int], bool], int]:
    """Awkwardly expressible.

    Count distinct (worker, chunk) per round, then post-process by
    comparing against the externally supplied expected total. Two pain
    points: (a) the dataflow language has no native concept of an
    ``expected set'' the operator can install at query-compile time, so
    the threshold must be passed in out of band; (b) the reducer's state
    grows to O(workers x chunks) per round to track the seen set,
    materially higher than Q2's O(workers) per round.
    """
    pipeline = Pipeline(
        Map(lambda p: ((p.job_id, p.round_id), (p.worker_id, p.chunk_id))),
        ReduceByKey(
            key_fn=lambda kv: kv[0],
            value_fn=lambda kv: frozenset([kv[1]]),
            agg_fn=lambda a, b: a | b,
            initial=frozenset(),
        ),
    )
    result = pipeline.run(packets)
    expected = expected_workers * expected_chunks
    return ({k: len(v) < expected for k, v in result}, pipeline.total_state)


def q4_univmon(packets) -> None:
    """NOT EXPRESSIBLE.

    Sketches estimate frequencies; they have no primitive for set
    cardinality compared against an external expected value. A distinct-
    count sketch (HyperLogLog) could estimate |observed set| but cannot
    enumerate which (worker, chunk) pairs are missing.
    """
    raise NotExpressible(
        "Comparing an observed multi-key set against an externally "
        "supplied expected cardinality is outside the sketch abstraction."
    )


# =============================================================================
# Q5: Cross-job interference detection
# =============================================================================
# Does job A's per-round latency correlate with job B's activity? This is
# the canonical cross-stream correlation query.
# =============================================================================

def _pearson(xs: List[float], ys: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = sum((x - mx) ** 2 for x in xs) ** 0.5
    dy = sum((y - my) ** 2 for y in ys) ** 0.5
    if dx == 0 or dy == 0:
        return 0.0
    return num / (dx * dy)


def q5_oracle(oracle: Oracle) -> Dict[Tuple[int, int], float]:
    # Compute per-(job, round) latency (reuse Q1 logic)
    state: Dict[Tuple[int, int], List[float]] = {}
    for p in oracle.packets:
        k = (p.job_id, p.round_id)
        if k not in state:
            state[k] = [p.timestamp, p.timestamp]
        else:
            if p.timestamp < state[k][0]:
                state[k][0] = p.timestamp
            if p.timestamp > state[k][1]:
                state[k][1] = p.timestamp
    latencies: Dict[int, Dict[int, float]] = defaultdict(dict)
    for (j, r), (t0, t1) in state.items():
        latencies[j][r] = t1 - t0

    jobs = sorted(latencies.keys())
    correlations: Dict[Tuple[int, int], float] = {}
    for i, ja in enumerate(jobs):
        for jb in jobs[i + 1:]:
            common = sorted(set(latencies[ja]) & set(latencies[jb]))
            if len(common) < 3:
                continue
            xs = [latencies[ja][r] for r in common]
            ys = [latencies[jb][r] for r in common]
            correlations[(ja, jb)] = _pearson(xs, ys)
    return correlations


def q5_sonata(packets) -> None:
    """NOT EXPRESSIBLE in Sonata's native operators.

    Sonata's pipeline composes operators over a single stream. The cross-
    job correlation query requires computing a derived metric (per-round
    latency) per job, joining across jobs by round identifier (which
    means by aligned time windows across jobs), and computing a
    statistical function (Pearson correlation) over the joined series.
    Sonata supports `zip` between two queries, but the correlation step
    cannot be expressed in the operator set; it must be performed by an
    external stream processor downstream of the query.
    """
    raise NotExpressible(
        "Cross-stream correlation requires a statistical operator "
        "(Pearson) outside Sonata's native operator set."
    )


def q5_univmon(packets) -> None:
    """NOT EXPRESSIBLE. Same reason as Q1: timestamps are not in the
    sketch abstraction."""
    raise NotExpressible(
        "Cross-stream correlation over per-round latencies requires "
        "timestamp tracking, which is outside the sketch abstraction."
    )
