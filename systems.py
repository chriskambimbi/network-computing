"""
Three telemetry systems under evaluation.

  Oracle           --- full-visibility ground truth (post-hoc analyzer).
  Pipeline         --- Sonata-style dataflow with filter/map/reduce/distinct.
  UnivMonSketch    --- universal sketch over a chosen flow key.

These are abstractions of the published systems, not the systems
themselves. The point is to evaluate the expressiveness of their query
models, not their end-to-end performance.
"""

from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, List, Tuple
import hashlib


# =============================================================================
# Oracle
# =============================================================================

class Oracle:
    """Full-visibility ground truth. Stores every packet for post-hoc
    arbitrary queries. Bounds the state cost any practical system would
    have to beat to be considered an improvement."""

    def __init__(self) -> None:
        self.packets: List = []

    def ingest(self, packets) -> None:
        for p in packets:
            self.packets.append(p)

    @property
    def state_entries(self) -> int:
        return len(self.packets)


# =============================================================================
# Sonata-style dataflow engine
# =============================================================================
#
# We implement five operators from Sonata's query language: filter, map,
# reduce_by_key, distinct, and window. A Pipeline composes operators
# left-to-right and emits when the input stream is exhausted (the
# "end-of-window" boundary). State is tracked per operator so we can
# report total state entries for resource-cost comparisons.
# =============================================================================

class Operator:
    """Base class for Sonata dataflow operators."""

    @property
    def state_entries(self) -> int:
        return 0


class Filter(Operator):
    def __init__(self, predicate: Callable[[Any], bool]) -> None:
        self.predicate = predicate

    def apply(self, stream: Iterator) -> Iterator:
        return (x for x in stream if self.predicate(x))


class Map(Operator):
    def __init__(self, fn: Callable[[Any], Any]) -> None:
        self.fn = fn

    def apply(self, stream: Iterator) -> Iterator:
        return (self.fn(x) for x in stream)


class ReduceByKey(Operator):
    """Group items by key and aggregate values. Emits final
    (key, value) pairs when the input stream is exhausted."""

    def __init__(
        self,
        key_fn: Callable[[Any], Any],
        value_fn: Callable[[Any], Any],
        agg_fn: Callable[[Any, Any], Any],
        initial: Any,
    ) -> None:
        self.key_fn = key_fn
        self.value_fn = value_fn
        self.agg_fn = agg_fn
        self.initial = initial
        self.state: dict = {}

    def apply(self, stream: Iterator) -> Iterator:
        for x in stream:
            k = self.key_fn(x)
            v = self.value_fn(x)
            self.state[k] = self.agg_fn(self.state.get(k, self.initial), v)
        for k, v in self.state.items():
            yield (k, v)

    @property
    def state_entries(self) -> int:
        return len(self.state)


class Distinct(Operator):
    """Emit only the first occurrence of each key."""

    def __init__(self, key_fn: Callable[[Any], Any] = lambda x: x) -> None:
        self.key_fn = key_fn
        self.seen: set = set()

    def apply(self, stream: Iterator) -> Iterator:
        for x in stream:
            k = self.key_fn(x)
            if k not in self.seen:
                self.seen.add(k)
                yield x

    @property
    def state_entries(self) -> int:
        return len(self.seen)


class Pipeline:
    """Sonata-style dataflow pipeline. Operators apply left-to-right."""

    def __init__(self, *operators: Operator) -> None:
        self.operators: List[Operator] = list(operators)

    def run(self, packets) -> list:
        stream: Iterator = iter(packets)
        for op in self.operators:
            stream = op.apply(stream)
        return list(stream)

    @property
    def total_state(self) -> int:
        return sum(op.state_entries for op in self.operators)


# =============================================================================
# Online/windowed monitors
# =============================================================================
#
# These monitors model the part missing from the offline Pipeline above:
# bounded windows, configured round deadlines, timeout emission, and sliding
# correlation over derived per-round metrics. They are still query-model
# abstractions, not a faithful Sonata runtime.
# =============================================================================

@dataclass(frozen=True)
class RoundFailureAlert:
    job_id: int
    round_id: int
    detected_at: float
    observed: int
    expected: int
    reason: str


def compute_inactivity_alert(
    *,
    job_id: int,
    round_id: int,
    round_start: float,
    events: List[Tuple[float, int]],
    expected: int,
    timeout_s: float,
    final_observed: int,
) -> Any:
    """Inactivity-timeout detection for one (job, round).

    Walks the event timeline in time order. Fires the moment an inter-event
    gap exceeds ``timeout_s`` while the round's running count is below the
    expected pair total. If no gap fires and the round still ends incomplete,
    fires ``timeout_s`` after the last event (or after ``round_start`` when
    no events were observed at all). Returns None for healthy rounds that
    never trigger.
    """
    running = 0
    last_t = round_start
    fired_at: Any = None
    for t, increment in events:
        if running < expected and t > last_t + timeout_s:
            fired_at = last_t + timeout_s
            break
        running += increment
        last_t = t
    if fired_at is None and running < expected:
        fired_at = last_t + timeout_s
    if fired_at is None:
        return None
    if running == 0:
        reason = "missing-round"
    elif final_observed >= expected:
        reason = "premature-timeout"
    else:
        reason = "missing-pairs"
    return RoundFailureAlert(
        job_id=job_id,
        round_id=round_id,
        detected_at=fired_at,
        observed=running,
        expected=expected,
        reason=reason,
    )


class OnlineRoundFailureMonitor:
    """Detect incomplete or absent rounds via an inactivity timeout.

    The monitor needs the expected round schedule up front. That is the point
    of the Q4 experiment: a completely failed round contains no packet to key
    on, so online telemetry needs an expected-set/expected-round primitive.

    Detection model: per (job, round), track the timestamp of the most recent
    observed activity and the running count. An alert fires at
    ``last_active_t + timeout_s`` if the round's observed count is still
    below the expected pair total at that point. Rounds with no activity at
    all fire at ``round_start + timeout_s`` (the monitor treats the scheduled
    round start as the implicit baseline activity time). If a later event
    arrives before the timeout would have fired, ``last_active_t`` advances
    and the prospective alert is suppressed.

    Because the close() pass sees the full timeline post-hoc, it is
    equivalent to an online simulation that processes events in time order.
    """

    def __init__(self, jobs: Iterable, timeout_s: float = 0.005) -> None:
        self.jobs: Dict[int, Any] = {job.job_id: job for job in jobs}
        self.timeout_s = timeout_s
        self.observed_counts: Dict[Tuple[int, int], int] = defaultdict(int)
        self.events: Dict[Tuple[int, int], List[Tuple[float, int]]] = defaultdict(list)
        self.last_active_t: Dict[Tuple[int, int], float] = {}
        self.max_state_entries = 0

    def expected_pairs(self, job_id: int) -> int:
        job = self.jobs[job_id]
        return job.n_workers * job.n_chunks

    def round_start(self, job_id: int, round_id: int) -> float:
        job = self.jobs[job_id]
        return (
            job.start_offset_s
            + round_id * (job.round_duration_s + job.inter_round_gap_s)
        )

    def round_end(self, job_id: int, round_id: int) -> float:
        job = self.jobs[job_id]
        return self.round_start(job_id, round_id) + job.round_duration_s

    def deadline(self, job_id: int, round_id: int) -> float:
        return self.round_end(job_id, round_id) + self.timeout_s

    def observe_packet(self, packet) -> None:
        key = (packet.job_id, packet.round_id)
        self.observed_counts[key] += 1
        self.events[key].append((float(packet.timestamp), 1))
        self.max_state_entries = max(self.max_state_entries, self.observed_counts[key])

    def observe_round_count(
        self,
        job_id: int,
        round_id: int,
        observed_pairs: int,
        t: Any = None,
    ) -> None:
        """Record one batched arrival for (job_id, round_id).

        ``t`` is the wall-clock time of the arrival. If omitted the monitor
        treats the round's scheduled end as the arrival time (legacy
        deadline-style behaviour preserved for callers that do not model
        intra-round timing).
        """
        key = (job_id, round_id)
        self.observed_counts[key] += observed_pairs
        if t is None:
            t = self.round_end(job_id, round_id)
        self.events[key].append((float(t), int(observed_pairs)))
        self.max_state_entries = max(self.max_state_entries, self.observed_counts[key])

    def close(self) -> List[RoundFailureAlert]:
        alerts: List[RoundFailureAlert] = []
        for job in self.jobs.values():
            expected = self.expected_pairs(job.job_id)
            for round_id in range(job.n_rounds):
                key = (job.job_id, round_id)
                events = sorted(self.events.get(key, []))
                final_observed = self.observed_counts.get(key, 0)
                alert = compute_inactivity_alert(
                    job_id=job.job_id,
                    round_id=round_id,
                    round_start=self.round_start(job.job_id, round_id),
                    events=events,
                    expected=expected,
                    timeout_s=self.timeout_s,
                    final_observed=final_observed,
                )
                if alert is not None:
                    alerts.append(alert)
        return alerts

    @property
    def state_entries(self) -> int:
        if not self.jobs:
            return self.max_state_entries
        # An exact Sonata-style distinct-set implementation needs to be able
        # to represent every expected (worker, chunk) pair for an active round.
        expected = max(self.expected_pairs(job_id) for job_id in self.jobs)
        return max(self.max_state_entries, expected)


@dataclass(frozen=True)
class CorrelationAlert:
    job_pair: Tuple[int, int]
    round_id: int
    detected_at: float
    score: float


class WindowedCorrelationMonitor:
    """Sliding-window cross-job correlation over per-round metrics.

    The optional ``export_latency_s`` models the lag between a switch emitting
    a per-round metric and a downstream correlator receiving it. In-network
    correlators (e.g. ``IncAwareMonitor.correlation_monitor``) pay zero; an
    off-switch dataflow downstream of Sonata pays the export pipeline's
    end-to-end delay.
    """

    def __init__(
        self,
        job_pair: Tuple[int, int],
        window_size: int = 8,
        threshold: float = 0.75,
        export_latency_s: float = 0.0,
    ) -> None:
        self.job_pair = job_pair
        self.window_size = window_size
        self.threshold = threshold
        self.export_latency_s = export_latency_s
        self.window = deque(maxlen=window_size)
        self.alerts: List[CorrelationAlert] = []

    @staticmethod
    def _pearson(xs: List[float], ys: List[float]) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        mx = sum(xs) / n
        my = sum(ys) / n
        dx = sum((x - mx) ** 2 for x in xs) ** 0.5
        dy = sum((y - my) ** 2 for y in ys) ** 0.5
        if dx == 0 or dy == 0:
            return 0.0
        return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (dx * dy)

    def observe_round(
        self,
        round_id: int,
        detected_at: float,
        victim_latency_s: float,
        interferer_active: bool,
    ) -> None:
        self.window.append((round_id, victim_latency_s, 1.0 if interferer_active else 0.0))
        if len(self.window) < self.window_size:
            return
        latencies = [x[1] for x in self.window]
        activity = [x[2] for x in self.window]
        score = self._pearson(latencies, activity)
        if score >= self.threshold:
            self.alerts.append(
                CorrelationAlert(
                    job_pair=self.job_pair,
                    round_id=round_id,
                    detected_at=detected_at + self.export_latency_s,
                    score=score,
                )
            )

    @property
    def state_entries(self) -> int:
        # latency + activity per retained round.
        return self.window_size * 2


class IncAwareMonitor:
    """Reference monitor with first-class INC round semantics.

    This is a strawman baseline, not a proposed system. It represents the
    primitives the paper argues are missing: configured jobs, expected
    participants/chunks, per-round state, and application-level job IDs.
    """

    def __init__(
        self,
        jobs: Iterable,
        timeout_s: float = 0.005,
        correlation_window: int = 8,
        correlation_threshold: float = 0.75,
    ) -> None:
        self.jobs: Dict[int, Any] = {job.job_id: job for job in jobs}
        self.timeout_s = timeout_s
        self.correlation_window = correlation_window
        self.correlation_threshold = correlation_threshold

    def expected_chunks(self, job_id: int) -> int:
        return self.jobs[job_id].n_chunks

    def expected_pairs(self, job_id: int) -> int:
        job = self.jobs[job_id]
        return job.n_workers * job.n_chunks

    def deadline(self, job_id: int, round_id: int) -> float:
        job = self.jobs[job_id]
        round_start = (
            job.start_offset_s
            + round_id * (job.round_duration_s + job.inter_round_gap_s)
        )
        return round_start + job.round_duration_s + self.timeout_s

    def lossy_workers_by_round(
        self, counts: Dict[Tuple[int, int, int], int]
    ) -> Dict[Tuple[int, int], int]:
        by_round: Dict[Tuple[int, int], Dict[int, int]] = defaultdict(dict)
        for (job_id, round_id, worker_id), count in counts.items():
            by_round[(job_id, round_id)][worker_id] = count

        result: Dict[Tuple[int, int], int] = {}
        for key, worker_counts in by_round.items():
            job_id, _ = key
            expected = self.expected_chunks(job_id)
            missing = {
                worker: count
                for worker, count in worker_counts.items()
                if count < expected
            }
            if missing:
                result[key] = min(missing, key=missing.get)
        return result

    def stragglers_by_round(self, packets) -> Dict[Tuple[int, int], int]:
        last: Dict[Tuple[int, int], Tuple[float, int]] = defaultdict(
            lambda: (float("-inf"), -1)
        )
        for packet in packets:
            key = (packet.job_id, packet.round_id)
            if packet.timestamp > last[key][0]:
                last[key] = (packet.timestamp, packet.worker_id)
        return {key: value[1] for key, value in last.items()}

    def round_failure_alerts_from_counts(
        self, observed_counts: Dict[Tuple[int, int], int]
    ) -> List[RoundFailureAlert]:
        alerts: List[RoundFailureAlert] = []
        for job in self.jobs.values():
            expected = self.expected_pairs(job.job_id)
            for round_id in range(job.n_rounds):
                key = (job.job_id, round_id)
                observed = observed_counts.get(key, 0)
                if observed >= expected:
                    continue
                alerts.append(
                    RoundFailureAlert(
                        job_id=job.job_id,
                        round_id=round_id,
                        detected_at=self.deadline(job.job_id, round_id),
                        observed=observed,
                        expected=expected,
                        reason="missing-round" if observed == 0 else "missing-pairs",
                    )
                )
        return alerts

    def round_failure_alerts_from_events(
        self,
        events_by_round: Dict[Tuple[int, int], List[Tuple[float, int]]],
    ) -> List[RoundFailureAlert]:
        """Inactivity-timeout detection over per-round event timelines.

        Mirrors OnlineRoundFailureMonitor.close() but uses the INC-aware
        monitor's configured ``timeout_s`` and per-job round-start derivation.
        """
        alerts: List[RoundFailureAlert] = []
        for job in self.jobs.values():
            expected = self.expected_pairs(job.job_id)
            for round_id in range(job.n_rounds):
                key = (job.job_id, round_id)
                events = sorted(events_by_round.get(key, []))
                final_observed = sum(c for _, c in events)
                round_start = (
                    job.start_offset_s
                    + round_id * (job.round_duration_s + job.inter_round_gap_s)
                )
                alert = compute_inactivity_alert(
                    job_id=job.job_id,
                    round_id=round_id,
                    round_start=round_start,
                    events=events,
                    expected=expected,
                    timeout_s=self.timeout_s,
                    final_observed=final_observed,
                )
                if alert is not None:
                    alerts.append(alert)
        return alerts

    def correlation_monitor(self, job_pair: Tuple[int, int]) -> WindowedCorrelationMonitor:
        return WindowedCorrelationMonitor(
            job_pair=job_pair,
            window_size=self.correlation_window,
            threshold=self.correlation_threshold,
        )

    def q2_state_entries(self) -> int:
        if not self.jobs:
            return 0
        return max(job.n_workers + 1 for job in self.jobs.values())

    def q3_state_entries(self) -> int:
        return 1

    def q4_state_entries(self) -> int:
        if not self.jobs:
            return 0
        return max(job.n_workers + job.n_chunks for job in self.jobs.values())


# =============================================================================
# UnivMon-style sketch
# =============================================================================
#
# We implement Count-Sketch (Charikar, Chen, Farach-Colton 2002) and use it
# as the underlying primitive for a simplified UnivMon: a hierarchy of
# Count-Sketches over recursively sampled streams. UnivMon's contribution
# was unifying many G-sum sketches under one universal structure; for the
# purposes of our paper, what matters is that the abstraction returns
# frequency estimates over a flow key.
# =============================================================================

class CountSketch:
    """A single Count-Sketch table of (width x depth) signed counters."""

    def __init__(self, width: int = 1024, depth: int = 5, seed: int = 42):
        self.width = width
        self.depth = depth
        self.table: List[List[int]] = [[0] * width for _ in range(depth)]
        self.seed = seed
        # We track observed keys to enable point queries; in practice
        # operators only get back heavy-hitter outputs, but for evaluation
        # we want to compare estimates against the ground-truth set.
        self.observed_keys: set = set()

    def _hash(self, key, row: int) -> Tuple[int, int]:
        h = hashlib.md5(f"{key}-{row}-{self.seed}".encode()).digest()
        idx = int.from_bytes(h[:4], "little") % self.width
        sign = 1 if (h[4] & 1) else -1
        return idx, sign

    def update(self, key, count: int = 1) -> None:
        self.observed_keys.add(key)
        for row in range(self.depth):
            idx, sign = self._hash(key, row)
            self.table[row][idx] += sign * count

    def estimate(self, key) -> int:
        """Median-of-rows estimator."""
        ests = []
        for row in range(self.depth):
            idx, sign = self._hash(key, row)
            ests.append(sign * self.table[row][idx])
        ests.sort()
        return ests[len(ests) // 2]

    def heavy_hitters(self, threshold: int) -> List[Tuple]:
        return [
            (k, self.estimate(k))
            for k in self.observed_keys
            if self.estimate(k) >= threshold
        ]

    @property
    def state_entries(self) -> int:
        return self.width * self.depth


def _hash_to_level(key, n_levels: int) -> int:
    """Recursive sampling: count trailing zero-bits of hash(key) to assign
    a level in the sketch hierarchy. Geometric distribution over levels."""
    h = int.from_bytes(hashlib.md5(str(key).encode()).digest()[:4], "little")
    bits = bin(h)[2:].zfill(32)
    level = 0
    for b in reversed(bits):
        if b == "1":
            break
        level += 1
    return min(level, n_levels - 1)


class UnivMonSketch:
    """Simplified UnivMon: a hierarchy of Count-Sketches over recursively
    sampled streams. Each key is sampled into levels [0, k] where k is
    determined by trailing-zero count of hash(key).

    Operators choose a flow-key function. The choice of flow key is itself
    a design decision that affects what questions become askable.
    """

    def __init__(
        self,
        levels: int = 8,
        width: int = 1024,
        depth: int = 5,
        seed: int = 42,
        key_fn: Callable = None,
    ) -> None:
        self.levels = levels
        self.sketches: List[CountSketch] = [
            CountSketch(width, depth, seed + i) for i in range(levels)
        ]
        self.key_fn = key_fn or (lambda p: (p.job_id, p.worker_id))

    def ingest(self, packets) -> None:
        for p in packets:
            key = self.key_fn(p)
            max_level = _hash_to_level(key, self.levels)
            for l in range(max_level + 1):
                self.sketches[l].update(key)

    def heavy_hitters(self, threshold: int) -> List[Tuple]:
        return self.sketches[0].heavy_hitters(threshold)

    def estimate(self, key) -> int:
        return self.sketches[0].estimate(key)

    @property
    def state_entries(self) -> int:
        return sum(s.state_entries for s in self.sketches)
