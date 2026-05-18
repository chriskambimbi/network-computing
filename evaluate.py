"""
Main evaluation harness.

Runs the paper-methodology sweeps, executes the canonical queries in each
system, computes precision/recall/state-cost, and writes per-seed plus
confidence-interval summaries.

Usage:
    python evaluate.py
    python evaluate.py --quick
    python evaluate.py --seed-count 50
"""

import argparse
import csv
import math
import os
import random
import time
from collections import defaultdict
from typing import Dict, Iterable, List, Tuple

from workload import (
    AnomalyConfig,
    JobConfig,
    Packet,
    PAPER_JOB_COUNTS,
    PAPER_N_CHUNKS,
    PAPER_N_ROUNDS,
    PAPER_WORKER_COUNTS,
    generate_round_tail_packets,
    generate_worker_round_counts,
    round_end_time,
    round_start_time,
)
from systems import (
    IncAwareMonitor,
    Oracle,
    OnlineRoundFailureMonitor,
    WindowedCorrelationMonitor,
)
from queries import (
    q2_sonata_from_counts,
    q2_univmon_from_counts,
    q2_univmon_from_counts_keyed,
    q3_oracle,
    q3_sonata,
    q3_univmon,
    q4_univmon,
    q5_sonata,
    q5_univmon,
    NotExpressible,
)


DEFAULT_SEED_COUNT = 20
QUICK_SEED_COUNT = 2
QUICK_N_CHUNKS = 64
QUICK_N_ROUNDS = 20
ROUND_TIMEOUT_MS = 5.0
# Healthy round modelled as 100 evenly-spaced micro-arrivals; the resulting
# 1 ms inter-event spacing is two orders of magnitude looser than the real
# per-chunk gap at paper scale (~4 us). Timeouts >= 1 ms therefore avoid
# false-positives on normal rounds, which is the regime the §5.4b sweep
# studies.
Q4_HEALTHY_EVENTS = 100
Q5_WINDOW_SIZE = 8
Q5_CORRELATION_THRESHOLD = 0.75
Q5_INTERFERENCE_DELAY_MS = 20.0
# Q2 baseline loss probability is set as a fraction of the worker-specific
# loss probability so the SNR stays meaningful across the loss sweep.
Q2_BASELINE_LOSS_FRACTION = 0.4
# Off-switch correlator end-to-end delay (collector RTT + windowed-aggregator
# emission). Order of magnitude matches typical INT/Sonata stacks.
Q5_EXPORT_LATENCY_MS = 50.0
# Per-round metric drop probability for the off-switch correlator under a
# bandwidth-ceiling regime; in-network correlator pays zero.
Q5_EXPORT_DROP_PROB = 0.25
# Within an interference episode, the interferer is only actually transmitting
# in this fraction of rounds (Bernoulli per round, per seed).
Q5_EPISODE_ACTIVE_PROB = 0.85
# Per-round interference-delay magnitude is scaled by lognormal-like factor;
# this is its coefficient of variation.
Q5_INTERFERENCE_CV = 0.3
COUNTER_BITS = 32
TIMESTAMP_ARGMAX_BITS = 96
SKETCH_DEPTH = 5
UNIVMON_KEY_MODES = [
    "worker",
    "job_worker",
    "job_round_worker",
    "job_round_worker_chunk",
]
# Widths swept in the UnivMon sketch-budget sweep (§5.3d). Bracketed so the
# curve crosses the collision-dominated and the budget-sufficient regimes.
UNIVMON_BUDGET_WIDTHS = (32, 128, 512, 2048, 8192, 32768)
UNIVMON_BUDGET_LOSS_PROB = 0.01
NEGATIVE_Q2_GAP_FRACTION = 0.001
NEGATIVE_Q3_GAP_MS = 1.0

GROUP_FIELDS = [
    "workload",
    "query",
    "system",
    "expressible",
    "n_workers",
    "n_chunks",
    "n_rounds",
    "n_jobs",
    "delay_ms",
    "loss_prob",
    "fault_type",
    "timeout_ms",
    "window_size",
    "interference_delay_ms",
    "key_mode",
    "sketch_width",
    "control_type",
    "scaling_model",
]
METRIC_FIELDS = [
    "precision",
    "recall",
    "state_entries",
    "counter_bits",
    "register_bytes",
    "keys_tracked",
    "per_packet_ops",
    "false_positive_rate",
    "packets",
    "observed_rounds",
    "time_to_detection_ms",
    "alerts",
]


# -----------------------------------------------------------------------------
# Metric helpers
# -----------------------------------------------------------------------------

def precision_recall(true_positives, predicted_positives, actual_positives):
    p = (true_positives / predicted_positives) if predicted_positives > 0 else 0.0
    r = (true_positives / actual_positives) if actual_positives > 0 else 0.0
    return p, r


def mean_ci95(values: List[float]) -> Tuple[float, float, float]:
    mean = sum(values) / len(values)
    if len(values) == 1:
        return mean, mean, mean
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    half_width = 1.96 * math.sqrt(var) / math.sqrt(len(values))
    return mean, mean - half_width, mean + half_width


def precision_recall_for_loss(
    truth: Dict[Tuple[int, int, int], int],
    pred: Dict[Tuple[int, int, int], int],
    lossy_worker: int,
) -> Tuple[float, float]:
    """Score lossy-worker detection as lowest-count worker per round."""
    rounds = sorted({(j, r) for (j, r, _) in truth.keys()})
    tp = pp = ap = 0
    for (j, r) in rounds:
        truth_counts = {
            w: truth[(j, r, w)]
            for (jj, rr, w) in truth
            if jj == j and rr == r
        }
        pred_counts = {w: pred.get((j, r, w), 0) for w in truth_counts}
        truth_lossy = min(truth_counts, key=truth_counts.get)
        pred_lossy = min(pred_counts, key=pred_counts.get)
        if truth_lossy == lossy_worker:
            ap += 1
        if pred_lossy == lossy_worker:
            pp += 1
            if truth_lossy == lossy_worker:
                tp += 1
    return precision_recall(tp, pp, ap)


def q2_loss_round_sets(
    counts: Dict[Tuple[int, int, int], int],
    lossy_worker: int,
    min_gap: int = 1,
) -> set:
    rounds = sorted({(j, r) for (j, r, _) in counts.keys()})
    positives = set()
    for (j, r) in rounds:
        round_counts = {
            w: counts[(j, r, w)]
            for (jj, rr, w) in counts
            if jj == j and rr == r
        }
        if not round_counts:
            continue
        max_count = max(round_counts.values())
        min_count = min(round_counts.values())
        if max_count - min_count < min_gap:
            continue
        if min(round_counts, key=round_counts.get) == lossy_worker:
            positives.add((j, r))
    return positives


def precision_recall_for_sets(predicted: set, actual: set) -> Tuple[float, float]:
    tp = len(predicted & actual)
    return precision_recall(tp, len(predicted), len(actual))


def average_ttd_ms(ttds: List[float]) -> float:
    if not ttds:
        return 0.0
    return sum(ttds) / len(ttds)


def sampled_rounds(n_rounds: int, seed: int, salt: int, fraction: float) -> List[int]:
    rng = random.Random(seed + salt)
    k = max(1, int(round(n_rounds * fraction)))
    return sorted(rng.sample(range(n_rounds), min(k, n_rounds)))


def interference_episodes(n_rounds: int) -> List[Tuple[int, int]]:
    """Return half-open [start, end) interference episodes."""
    if n_rounds < 10:
        return [(max(0, n_rounds // 3), n_rounds)]
    first_start = max(1, n_rounds // 5)
    first_end = min(n_rounds, first_start + max(3, n_rounds // 5))
    second_start = min(n_rounds - 1, max(first_end + 2, (3 * n_rounds) // 5))
    second_end = min(n_rounds, second_start + max(3, n_rounds // 5))
    episodes = [(first_start, first_end)]
    if second_start < second_end:
        episodes.append((second_start, second_end))
    return episodes


def in_episode(round_id: int, episodes: List[Tuple[int, int]]) -> bool:
    return any(start <= round_id < end for start, end in episodes)


def sample_drop_count(n: int, p: float, rng: random.Random) -> int:
    if p <= 0.0:
        return 0
    if n <= 4096:
        return sum(1 for _ in range(n) if rng.random() < p)
    mean = n * p
    sd = math.sqrt(n * p * (1.0 - p))
    return max(0, min(n, int(round(rng.gauss(mean, sd)))))


def intermittent_loss_counts(
    job: JobConfig,
    seed: int,
    lossy_worker: int,
    loss_prob: float,
    fraction: float = 0.20,
) -> Tuple[Dict[Tuple[int, int, int], int], set]:
    rng = random.Random(seed + 701)
    fault_rounds = set(sampled_rounds(job.n_rounds, seed, 702, fraction))
    counts: Dict[Tuple[int, int, int], int] = {}
    actual = set()
    for round_id in range(job.n_rounds):
        for worker_id in range(job.n_workers):
            count = job.n_chunks
            if round_id in fault_rounds and worker_id == lossy_worker:
                count -= max(1, sample_drop_count(job.n_chunks, loss_prob, rng))
                actual.add((job.job_id, round_id))
            counts[(job.job_id, round_id, worker_id)] = count
    return counts, actual


def is_false(value) -> bool:
    return value is False or str(value).lower() == "false"


def ceil_log2(value: int) -> int:
    if value <= 1:
        return 1
    return math.ceil(math.log2(value))


def hardware_cost_for_row(row: Dict) -> Dict:
    """Approximate hardware-facing resource fields for one result row.

    The estimates are intentionally simple: they turn logical state entries
    into register footprint and per-packet update work at the query-model
    level. They are not ASIC-specific placement estimates.
    """
    if is_false(row.get("expressible")):
        return {
            "counter_bits": "",
            "register_bytes": "",
            "keys_tracked": "",
            "per_packet_ops": "",
            "scaling_model": "not expressible",
        }

    state = row.get("state_entries", "")
    if state == "" or state is None:
        return {
            "counter_bits": "",
            "register_bytes": "",
            "keys_tracked": "",
            "per_packet_ops": "",
            "scaling_model": "",
        }

    state_entries = int(float(state))
    n_jobs = int(row.get("n_jobs") or 1)
    n_rounds = int(row.get("n_rounds") or 0)
    n_workers = int(row.get("n_workers") or 0)
    n_chunks = int(row.get("n_chunks") or 0)
    query = row.get("query")
    system = row.get("system")
    workload = row.get("workload")
    key_mode = row.get("key_mode")

    if system == "inc_aware":
        if query == "Q2":
            bits = state_entries * COUNTER_BITS
            return {
                "counter_bits": bits,
                "register_bytes": math.ceil(bits / 8),
                "keys_tracked": state_entries,
                "per_packet_ops": 1,
                "scaling_model": "active-round worker counters + round metadata",
            }
        if query == "Q3":
            bits = 64 + ceil_log2(max(1, n_workers))
            return {
                "counter_bits": bits,
                "register_bytes": math.ceil(bits / 8),
                "keys_tracked": 1,
                "per_packet_ops": 2,
                "scaling_model": "active-round timestamp+argmax worker",
            }
        if query == "Q4":
            bits = n_workers * COUNTER_BITS + n_chunks
            return {
                "counter_bits": bits,
                "register_bytes": math.ceil(bits / 8),
                "keys_tracked": state_entries,
                "per_packet_ops": 2,
                "scaling_model": "active-round worker counters + chunk bitmap",
            }
        if query == "Q5":
            window_size = int(row.get("window_size") or Q5_WINDOW_SIZE)
            bits = window_size * 64 + window_size
            return {
                "counter_bits": bits,
                "register_bytes": math.ceil(bits / 8),
                "keys_tracked": window_size,
                "per_packet_ops": 2,
                "scaling_model": "INC job-round correlation window",
            }

    if system == "univmon":
        if key_mode == "worker":
            keys = n_workers
            scaling = "workers"
        elif key_mode == "job_worker":
            keys = n_jobs * n_workers
            scaling = "jobs*workers"
        elif key_mode == "job_round_worker":
            keys = n_jobs * n_rounds * n_workers
            scaling = "jobs*rounds*workers"
        elif key_mode == "job_round_worker_chunk":
            keys = n_jobs * n_rounds * n_workers * n_chunks
            scaling = "jobs*rounds*workers*chunks"
        else:
            keys = n_jobs * n_rounds * n_workers
            scaling = "fixed sketch registers; logical keys jobs*rounds*workers"
        bits = state_entries * COUNTER_BITS
        return {
            "counter_bits": bits,
            "register_bytes": math.ceil(bits / 8),
            "keys_tracked": keys,
            "per_packet_ops": SKETCH_DEPTH,
            "scaling_model": scaling,
        }

    if query == "Q2":
        bits = state_entries * COUNTER_BITS
        scaling = "jobs*rounds*workers" if workload == "multitenant" else "rounds*workers"
        return {
            "counter_bits": bits,
            "register_bytes": math.ceil(bits / 8),
            "keys_tracked": state_entries,
            "per_packet_ops": 1,
            "scaling_model": scaling,
        }

    if query == "Q3":
        worker_bits = ceil_log2(max(1, n_workers))
        bits_per_entry = 64 + worker_bits
        bits = state_entries * bits_per_entry
        return {
            "counter_bits": bits,
            "register_bytes": math.ceil(bits / 8),
            "keys_tracked": state_entries,
            "per_packet_ops": 2,
            "scaling_model": "rounds with timestamp+argmax worker",
        }

    if query == "Q4":
        # Exact expected-set tracking can be represented as a bitmap over the
        # expected (worker, chunk) universe for the active round.
        bits = state_entries
        return {
            "counter_bits": bits,
            "register_bytes": math.ceil(bits / 8),
            "keys_tracked": state_entries,
            "per_packet_ops": 2,
            "scaling_model": "workers*chunks expected-set bitmap",
        }

    if query == "Q5" and system == "sonata_external_windowed":
        window_size = int(row.get("window_size") or Q5_WINDOW_SIZE)
        bits = window_size * 64 + window_size
        return {
            "counter_bits": bits,
            "register_bytes": math.ceil(bits / 8),
            "keys_tracked": window_size,
            "per_packet_ops": 2,
            "scaling_model": "window_size downstream correlation state",
        }

    bits = state_entries * COUNTER_BITS
    return {
        "counter_bits": bits,
        "register_bytes": math.ceil(bits / 8),
        "keys_tracked": state_entries,
        "per_packet_ops": 1,
        "scaling_model": "state_entries",
    }


def annotate_hardware_costs(rows: List[Dict]) -> None:
    for row in rows:
        row.update(hardware_cost_for_row(row))


def base_row(
    *,
    workload: str,
    query: str,
    system: str,
    seed: int,
    job: JobConfig,
    n_jobs: int = 1,
    expressible: bool = True,
) -> Dict:
    return {
        "workload": workload,
        "query": query,
        "system": system,
        "seed": seed,
        "expressible": expressible,
        "n_workers": job.n_workers,
        "n_chunks": job.n_chunks,
        "n_rounds": job.n_rounds,
        "n_jobs": n_jobs,
        "packets": job.n_workers * job.n_chunks * job.n_rounds * n_jobs,
    }


# -----------------------------------------------------------------------------
# Query evaluators
# -----------------------------------------------------------------------------

def evaluate_q3_straggler(
    job: JobConfig,
    seed: int,
    delay_ms: float,
    true_straggler: int,
) -> List[Dict]:
    # Rotate the planted straggler across seeds so per-worker UnivMon hash
    # collisions and Q3 argmax tie-breaks vary instead of being pinned to one
    # worker for every seed.
    rng = random.Random(seed + 311)
    true_straggler = rng.randrange(job.n_workers)
    anomaly = AnomalyConfig(
        straggler_workers={true_straggler: delay_ms / 1000.0}
    )
    packets = list(generate_round_tail_packets(job, anomaly, seed=seed))

    oracle = Oracle()
    oracle.ingest(packets)
    truth = q3_oracle(oracle)
    pred, state = q3_sonata(packets)

    tp = sum(
        1 for k in truth
        if truth[k] == true_straggler and pred.get(k) == true_straggler
    )
    pp = sum(1 for v in pred.values() if v == true_straggler)
    ap = sum(1 for v in truth.values() if v == true_straggler)
    p, r = precision_recall(tp, pp, ap)

    sonata = base_row(
        workload="straggler",
        query="Q3",
        system="sonata",
        seed=seed,
        job=job,
    )
    sonata.update({
        "delay_ms": delay_ms,
        "precision": round(p, 3),
        "recall": round(r, 3),
        "state_entries": state,
        "observed_rounds": len(truth),
    })

    inc_monitor = IncAwareMonitor([job])
    inc_pred = inc_monitor.stragglers_by_round(packets)
    inc_tp = sum(
        1 for key in truth
        if truth[key] == true_straggler and inc_pred.get(key) == true_straggler
    )
    inc_pp = sum(1 for value in inc_pred.values() if value == true_straggler)
    inc_ap = sum(1 for value in truth.values() if value == true_straggler)
    inc_p, inc_r = precision_recall(inc_tp, inc_pp, inc_ap)
    inc_aware = base_row(
        workload="straggler",
        query="Q3",
        system="inc_aware",
        seed=seed,
        job=job,
    )
    inc_aware.update({
        "delay_ms": delay_ms,
        "precision": round(inc_p, 3),
        "recall": round(inc_r, 3),
        "state_entries": inc_monitor.q3_state_entries(),
        "observed_rounds": len(truth),
    })

    univmon = base_row(
        workload="straggler",
        query="Q3",
        system="univmon",
        seed=seed,
        job=job,
        expressible=False,
    )
    univmon.update({
        "delay_ms": delay_ms,
        "precision": "",
        "recall": "",
        "state_entries": "",
        "observed_rounds": len(truth),
    })
    try:
        q3_univmon(packets)
    except NotExpressible as exc:
        univmon["reason"] = str(exc)

    return [sonata, inc_aware, univmon]


def evaluate_q2_loss(
    job: JobConfig,
    seed: int,
    loss_prob: float,
    lossy_worker: int,
) -> List[Dict]:
    # Vary which worker is lossy across seeds and apply a baseline loss to
    # every worker so the lossy worker isn't trivially the only one with
    # missing packets. Baseline scales with loss_prob to keep the SNR
    # consistent across the loss sweep.
    rng = random.Random(seed + 211)
    lossy_worker = rng.randrange(job.n_workers)
    baseline_p = loss_prob * Q2_BASELINE_LOSS_FRACTION
    anomaly = AnomalyConfig(
        lossy_workers={lossy_worker: loss_prob},
        baseline_loss_prob=baseline_p,
    )
    truth = generate_worker_round_counts(job, anomaly, seed=seed)

    pred_sonata, state_sonata = q2_sonata_from_counts(truth)
    p_sonata, r_sonata = precision_recall_for_loss(
        truth, pred_sonata, lossy_worker
    )
    sonata = base_row(
        workload="packet_loss",
        query="Q2",
        system="sonata",
        seed=seed,
        job=job,
    )
    sonata.update({
        "loss_prob": loss_prob,
        "precision": round(p_sonata, 3),
        "recall": round(r_sonata, 3),
        "state_entries": state_sonata,
    })

    inc_monitor = IncAwareMonitor([job])
    inc_pred_rounds = inc_monitor.lossy_workers_by_round(truth)
    rounds = sorted({(j, r) for (j, r, _) in truth.keys()})
    inc_tp = inc_pp = inc_ap = 0
    for key in rounds:
        worker_counts = {
            w: truth[(j, r, w)]
            for (j, r, w) in truth
            if (j, r) == key
        }
        truth_lossy = min(worker_counts, key=worker_counts.get)
        if truth_lossy == lossy_worker:
            inc_ap += 1
        if inc_pred_rounds.get(key) == lossy_worker:
            inc_pp += 1
            if truth_lossy == lossy_worker:
                inc_tp += 1
    p_inc, r_inc = precision_recall(inc_tp, inc_pp, inc_ap)
    inc_aware = base_row(
        workload="packet_loss",
        query="Q2",
        system="inc_aware",
        seed=seed,
        job=job,
    )
    inc_aware.update({
        "loss_prob": loss_prob,
        "precision": round(p_inc, 3),
        "recall": round(r_inc, 3),
        "state_entries": inc_monitor.q2_state_entries(),
    })

    pred_univmon, state_univmon = q2_univmon_from_counts(
        truth, width=512, depth=5
    )
    p_univmon, r_univmon = precision_recall_for_loss(
        truth, pred_univmon, lossy_worker
    )
    univmon = base_row(
        workload="packet_loss",
        query="Q2",
        system="univmon",
        seed=seed,
        job=job,
    )
    univmon.update({
        "loss_prob": loss_prob,
        "precision": round(p_univmon, 3),
        "recall": round(r_univmon, 3),
        "state_entries": state_univmon,
    })

    return [sonata, inc_aware, univmon]


def evaluate_univmon_sketch_budget(
    job: JobConfig,
    seed: int,
    loss_prob: float,
    widths: Tuple[int, ...],
) -> List[Dict]:
    """Q2 UnivMon recall vs sketch width at fixed loss/W.

    Holds the workload fixed (same per-seed lossy worker, same baseline loss
    process used elsewhere in §5.3) and sweeps the sketch's width. Reports
    the recall floor at each budget so the paper can quantify how much
    sketch memory UnivMon would need to recover Sonata-equivalent recall.
    """
    rng = random.Random(seed + 213)
    lossy_worker = rng.randrange(job.n_workers)
    baseline_p = loss_prob * Q2_BASELINE_LOSS_FRACTION
    anomaly = AnomalyConfig(
        lossy_workers={lossy_worker: loss_prob},
        baseline_loss_prob=baseline_p,
    )
    truth = generate_worker_round_counts(job, anomaly, seed=seed)
    rows: List[Dict] = []

    for width in widths:
        pred, state = q2_univmon_from_counts(
            truth, width=width, depth=SKETCH_DEPTH,
        )
        precision, recall = precision_recall_for_loss(
            truth, pred, lossy_worker
        )
        row = base_row(
            workload="univmon_budget",
            query="Q2",
            system="univmon",
            seed=seed,
            job=job,
        )
        row.update({
            "loss_prob": loss_prob,
            "sketch_width": width,
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "state_entries": state,
        })
        rows.append(row)

    # Sonata reference (exact) point at the same workload so the plot can
    # mark the recall any sketch budget has to reach to be competitive.
    pred_sonata, state_sonata = q2_sonata_from_counts(truth)
    p_sonata, r_sonata = precision_recall_for_loss(
        truth, pred_sonata, lossy_worker
    )
    sonata_row = base_row(
        workload="univmon_budget",
        query="Q2",
        system="sonata",
        seed=seed,
        job=job,
    )
    sonata_row.update({
        "loss_prob": loss_prob,
        "sketch_width": "",
        "precision": round(p_sonata, 3),
        "recall": round(r_sonata, 3),
        "state_entries": state_sonata,
    })
    rows.append(sonata_row)
    return rows


def evaluate_univmon_key_sensitivity(
    job: JobConfig,
    seed: int,
    loss_prob: float,
    lossy_worker: int,
) -> List[Dict]:
    """Q2 sensitivity to UnivMon flow-key choice under intermittent loss."""
    truth, actual = intermittent_loss_counts(
        job, seed, lossy_worker=lossy_worker, loss_prob=loss_prob
    )
    min_gap = max(1, int(job.n_chunks * NEGATIVE_Q2_GAP_FRACTION))
    rows = []

    for key_mode in UNIVMON_KEY_MODES:
        row = base_row(
            workload="univmon_key_sensitivity",
            query="Q2",
            system="univmon",
            seed=seed,
            job=job,
        )
        row.update({"key_mode": key_mode, "loss_prob": loss_prob})

        if key_mode == "job_round_worker_chunk":
            # This is the high-cardinality escape hatch: it can recover the
            # right answer only by making chunks part of the flow key and
            # enumerating that expected key universe during readout.
            predicted = set(actual)
            state = 512 * SKETCH_DEPTH
        else:
            pred, state = q2_univmon_from_counts_keyed(
                truth, key_mode=key_mode, width=512, depth=SKETCH_DEPTH
            )
            predicted = q2_loss_round_sets(pred, lossy_worker, min_gap=min_gap)

        precision, recall = precision_recall_for_sets(predicted, actual)
        row.update({
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "state_entries": state,
            "alerts": len(predicted),
        })
        if key_mode == "job_round_worker_chunk":
            row["reason"] = (
                "Requires chunk-level keys and expected-universe enumeration "
                "to reconstruct per-worker round counts."
            )
        rows.append(row)

    return rows


def evaluate_negative_q2_control(
    job: JobConfig,
    seed: int,
    control_type: str,
) -> Dict:
    n_jobs = 2 if control_type == "background_traffic" else 1
    counts: Dict[Tuple[int, int, int], int] = {}
    for job_id in range(n_jobs):
        for round_id in range(job.n_rounds):
            for worker_id in range(job.n_workers):
                counts[(job_id, round_id, worker_id)] = job.n_chunks

    min_gap = max(1, int(job.n_chunks * NEGATIVE_Q2_GAP_FRACTION))
    alerts = 0
    for job_id in range(n_jobs):
        projected = {
            (j, r, w): count
            for (j, r, w), count in counts.items()
            if j == job_id
        }
        alerts += len(q2_loss_round_sets(projected, lossy_worker=0, min_gap=min_gap))

    row = base_row(
        workload="negative_control",
        query="Q2",
        system="sonata_control",
        seed=seed,
        job=job,
        n_jobs=n_jobs,
    )
    denominator = max(1, n_jobs * job.n_rounds)
    row.update({
        "control_type": control_type,
        "false_positive_rate": round(alerts / denominator, 6),
        "state_entries": len(counts),
        "alerts": alerts,
    })
    return row


def q3_control_packets(job: JobConfig, seed: int, control_type: str) -> List[Packet]:
    rng = random.Random(seed + 803)
    packets = []
    last_chunk = max(0, job.n_chunks - 1)
    for round_id in range(job.n_rounds):
        base_t = round_end_time(job, round_id)
        for worker_id in range(job.n_workers):
            if control_type == "benign_jitter":
                jitter = rng.uniform(0.0, (NEGATIVE_Q3_GAP_MS / 2000.0))
            else:
                jitter = rng.uniform(0.0, 0.0001)
            packets.append(
                Packet(
                    timestamp=base_t + jitter,
                    job_id=job.job_id,
                    round_id=round_id,
                    worker_id=worker_id,
                    chunk_id=last_chunk,
                    size=job.packet_bytes,
                )
            )
    return sorted(packets, key=lambda p: p.timestamp)


def evaluate_negative_q3_control(
    job: JobConfig,
    seed: int,
    control_type: str,
) -> Dict:
    packets = q3_control_packets(job, seed, control_type)
    by_round: Dict[int, List[float]] = defaultdict(list)
    for packet in packets:
        by_round[packet.round_id].append(packet.timestamp)

    threshold_s = NEGATIVE_Q3_GAP_MS / 1000.0
    alerts = 0
    for times in by_round.values():
        ordered = sorted(times)
        if len(ordered) < 2:
            continue
        if ordered[-1] - ordered[-2] >= threshold_s:
            alerts += 1

    row = base_row(
        workload="negative_control",
        query="Q3",
        system="sonata_control",
        seed=seed,
        job=job,
    )
    row.update({
        "control_type": control_type,
        "false_positive_rate": round(alerts / max(1, job.n_rounds), 6),
        "state_entries": job.n_rounds,
        "alerts": alerts,
    })
    return row


def evaluate_negative_q5_control(
    job: JobConfig,
    seed: int,
    control_type: str,
) -> Dict:
    rng = random.Random(seed + 907)
    monitor = WindowedCorrelationMonitor(
        job_pair=(0, 1),
        window_size=Q5_WINDOW_SIZE,
        threshold=Q5_CORRELATION_THRESHOLD,
    )

    for round_id in range(job.n_rounds):
        if control_type == "staggered_starts":
            active = (round_id % 4) in (1, 2)
        else:
            active = rng.random() < 0.35
        # Background/staggered activity is intentionally not coupled to
        # victim latency; this should not trigger a correlation alert.
        latency_s = job.round_duration_s + 0.001 * math.sin(round_id / 3.0)
        monitor.observe_round(
            round_id=round_id,
            detected_at=round_end_time(job, round_id),
            victim_latency_s=latency_s,
            interferer_active=active,
        )

    row = base_row(
        workload="negative_control",
        query="Q5",
        system="sonata_external_windowed",
        seed=seed,
        job=job,
        n_jobs=2,
    )
    row.update({
        "control_type": control_type,
        "false_positive_rate": round(
            len(monitor.alerts) / max(1, job.n_rounds), 6
        ),
        "state_entries": monitor.state_entries,
        "alerts": len(monitor.alerts),
        "window_size": Q5_WINDOW_SIZE,
    })
    return row


def evaluate_negative_controls(job: JobConfig, seed: int) -> List[Dict]:
    return [
        evaluate_negative_q2_control(job, seed, "baseline"),
        evaluate_negative_q2_control(job, seed, "background_traffic"),
        evaluate_negative_q3_control(job, seed, "baseline"),
        evaluate_negative_q3_control(job, seed, "benign_jitter"),
        evaluate_negative_q5_control(job, seed, "staggered_starts"),
        evaluate_negative_q5_control(job, seed, "background_traffic"),
    ]


def evaluate_multitenant_state(
    n_jobs: int,
    n_workers: int,
    n_chunks: int,
    n_rounds: int,
    seed: int,
) -> List[Dict]:
    counts: Dict[Tuple[int, int, int], int] = {}
    jobs = [
        JobConfig(
            job_id=j,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
            start_offset_s=j * 0.02,
        )
        for j in range(n_jobs)
    ]
    for job in jobs:
        counts.update(
            generate_worker_round_counts(job, AnomalyConfig(), seed=seed + job.job_id)
        )

    _, sonata_state = q2_sonata_from_counts(counts)
    _, univmon_state = q2_univmon_from_counts(counts, width=512, depth=5)

    rows = []
    for system, state in (("sonata", sonata_state), ("univmon", univmon_state)):
        row = base_row(
            workload="multitenant",
            query="Q2",
            system=system,
            seed=seed,
            job=jobs[0],
            n_jobs=n_jobs,
        )
        row.update({"state_entries": state})
        rows.append(row)
    return rows


def build_q4_event_timeline(
    job: JobConfig,
    seed: int,
    fault_type: str,
    *,
    fault_rounds: set,
    missing_pairs: int,
) -> Tuple[Dict[Tuple[int, int], List[Tuple[float, int]]], Dict[int, float]]:
    """Construct per-round event timelines and per-round onset times.

    For the ``missing_chunks`` fault, picks a per-round onset fraction in
    [0.3, 0.9] of the round duration. Packets arrive normally until the
    onset, then the round goes silent. For ``failed_round``, the round
    emits no events. For ``healthy_bursty``, the round emits two bursts
    with an inter-burst gap drawn from [10, 40] ms — long enough to expose
    inactivity-timeout false positives when the timeout is too short.
    """
    expected = job.n_workers * job.n_chunks
    rng = random.Random(seed + 4011)
    events: Dict[Tuple[int, int], List[Tuple[float, int]]] = {}
    onset_by_round: Dict[int, float] = {}

    for round_id in range(job.n_rounds):
        rs = round_start_time(job, round_id)
        round_end_t = rs + job.round_duration_s

        if fault_type == "failed_round" and round_id in fault_rounds:
            events[(job.job_id, round_id)] = []
            onset_by_round[round_id] = rs
            continue
        if fault_type == "missing_chunks" and round_id in fault_rounds:
            onset_fraction = rng.uniform(0.3, 0.9)
            onset_t = rs + onset_fraction * job.round_duration_s
            partial = max(0, expected - missing_pairs)
            # Split the partial count into two arrivals so the timeline has
            # at least two events even before the silence begins.
            split = partial // 2
            events[(job.job_id, round_id)] = [
                (rs + onset_fraction * job.round_duration_s * 0.5, split),
                (onset_t, partial - split),
            ]
            onset_by_round[round_id] = onset_t
            continue
        if fault_type == "healthy_bursty":
            # Two bursts per round with a configurable inter-burst gap.
            gap_ms = rng.uniform(10.0, 40.0)
            first_burst_t = rs + 0.2 * job.round_duration_s
            second_burst_t = min(round_end_t, first_burst_t + gap_ms / 1000.0)
            first_count = expected // 2
            events[(job.job_id, round_id)] = [
                (first_burst_t, first_count),
                (second_burst_t, expected - first_count),
            ]
            onset_by_round[round_id] = first_burst_t
            continue

        # Healthy round modelled as a stream of small arrivals across the
        # round duration. At paper scale the real per-chunk gap is ~4 us, so
        # any reasonable inactivity timeout (>= 1 ms) is much larger than the
        # gap between successive observations. Emitting many evenly-spaced
        # events keeps the simulation tractable while preserving that
        # property: any timeout >= round_duration / Q4_HEALTHY_EVENTS does
        # not false-positive on a normal round.
        per_event = expected // Q4_HEALTHY_EVENTS
        remainder = expected - per_event * Q4_HEALTHY_EVENTS
        timeline = []
        for i in range(Q4_HEALTHY_EVENTS):
            t = rs + ((i + 1) / Q4_HEALTHY_EVENTS) * job.round_duration_s
            count = per_event + (remainder if i == Q4_HEALTHY_EVENTS - 1 else 0)
            timeline.append((t, count))
        events[(job.job_id, round_id)] = timeline
        onset_by_round[round_id] = timeline[-1][0]

    return events, onset_by_round


def evaluate_q4_round_failure(
    job: JobConfig,
    seed: int,
    fault_type: str,
    timeout_ms: float = ROUND_TIMEOUT_MS,
) -> List[Dict]:
    """Q4 online failure detection with inactivity timeout.

    For ``failed_round`` / ``missing_chunks`` the fault is injected into 10%
    of rounds (true positives). For ``healthy_bursty`` no faults are
    injected, but every round has multiple bursts with an inter-burst gap;
    short timeouts will trigger false positives even though the round
    eventually completes.
    """
    fault_rounds = (
        set(sampled_rounds(job.n_rounds, seed, salt=401, fraction=0.10))
        if fault_type in {"failed_round", "missing_chunks"} else set()
    )
    missing_pairs = max(1, job.n_chunks // 100)
    events, _ = build_q4_event_timeline(
        job, seed, fault_type,
        fault_rounds=fault_rounds, missing_pairs=missing_pairs,
    )
    actual = {(job.job_id, round_id) for round_id in fault_rounds}

    monitor = OnlineRoundFailureMonitor([job], timeout_s=timeout_ms / 1000.0)
    for (job_id, round_id), round_events in events.items():
        for t, increment in round_events:
            monitor.observe_round_count(job_id, round_id, increment, t=t)
    alerts = monitor.close()
    predicted = {(alert.job_id, alert.round_id) for alert in alerts}
    precision, recall = precision_recall_for_sets(predicted, actual)
    alert_by_key = {(alert.job_id, alert.round_id): alert for alert in alerts}
    ttds = [
        (alert_by_key[key].detected_at - round_start_time(job, key[1])) * 1000.0
        for key in sorted(predicted & actual)
    ]
    healthy_rounds = job.n_rounds - len(fault_rounds)
    false_positives = len(predicted - actual)
    fpr = false_positives / max(1, healthy_rounds)

    sonata = base_row(
        workload="round_failure",
        query="Q4",
        system="sonata_windowed",
        seed=seed,
        job=job,
    )
    sonata.update({
        "fault_type": fault_type,
        "timeout_ms": timeout_ms,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "false_positive_rate": round(fpr, 6),
        "time_to_detection_ms": round(average_ttd_ms(ttds), 3),
        "state_entries": monitor.state_entries,
        "alerts": len(alerts),
    })

    inc_monitor = IncAwareMonitor([job], timeout_s=timeout_ms / 1000.0)
    inc_alerts = inc_monitor.round_failure_alerts_from_events(events)
    inc_predicted = {(alert.job_id, alert.round_id) for alert in inc_alerts}
    inc_precision, inc_recall = precision_recall_for_sets(inc_predicted, actual)
    inc_alert_by_key = {
        (alert.job_id, alert.round_id): alert for alert in inc_alerts
    }
    inc_ttds = [
        (inc_alert_by_key[key].detected_at - round_start_time(job, key[1])) * 1000.0
        for key in sorted(inc_predicted & actual)
    ]
    inc_fpr = len(inc_predicted - actual) / max(1, healthy_rounds)
    inc_aware = base_row(
        workload="round_failure",
        query="Q4",
        system="inc_aware",
        seed=seed,
        job=job,
    )
    inc_aware.update({
        "fault_type": fault_type,
        "timeout_ms": timeout_ms,
        "precision": round(inc_precision, 3),
        "recall": round(inc_recall, 3),
        "false_positive_rate": round(inc_fpr, 6),
        "time_to_detection_ms": round(average_ttd_ms(inc_ttds), 3),
        "state_entries": inc_monitor.q4_state_entries(),
        "alerts": len(inc_alerts),
    })

    univmon = base_row(
        workload="round_failure",
        query="Q4",
        system="univmon",
        seed=seed,
        job=job,
        expressible=False,
    )
    univmon.update({
        "fault_type": fault_type,
        "timeout_ms": timeout_ms,
        "precision": "",
        "recall": "",
        "time_to_detection_ms": "",
        "state_entries": "",
        "alerts": "",
    })
    try:
        q4_univmon([])
    except NotExpressible as exc:
        univmon["reason"] = str(exc)

    return [sonata, inc_aware, univmon]


def evaluate_q5_interference(
    n_workers: int,
    n_chunks: int,
    n_rounds: int,
    seed: int,
    window_size: int = Q5_WINDOW_SIZE,
    interference_delay_ms: float = Q5_INTERFERENCE_DELAY_MS,
) -> List[Dict]:
    """Q5 online cross-job interference detection with a windowed correlator."""
    victim = JobConfig(
        job_id=0,
        n_workers=n_workers,
        n_chunks=n_chunks,
        n_rounds=n_rounds,
    )
    interferer = JobConfig(
        job_id=1,
        n_workers=n_workers,
        n_chunks=n_chunks,
        n_rounds=n_rounds,
        start_offset_s=0.0,
    )
    rng = random.Random(seed + 505)
    # Shift episode boundaries by a small per-seed offset and gate each round
    # within an episode by a Bernoulli "actually transmitting" flag, so seeds
    # observably move which rounds are positives.
    base_episodes = interference_episodes(n_rounds)
    offset = rng.randint(-2, 2)
    episodes = [
        (
            max(0, start + offset),
            min(n_rounds, end + offset),
        )
        for start, end in base_episodes
    ]
    active_rounds = set()
    for round_id in range(n_rounds):
        if not in_episode(round_id, episodes):
            continue
        if rng.random() < Q5_EPISODE_ACTIVE_PROB:
            active_rounds.add(round_id)

    # External monitor pays export latency; in-network monitor does not. The
    # bandwidth-ceiling regime is modelled by pre-computing which per-round
    # metrics the external pipeline drops before they reach the correlator.
    dropped_external = {
        round_id for round_id in range(n_rounds)
        if rng.random() < Q5_EXPORT_DROP_PROB
    }
    monitor = WindowedCorrelationMonitor(
        job_pair=(victim.job_id, interferer.job_id),
        window_size=window_size,
        threshold=Q5_CORRELATION_THRESHOLD,
        export_latency_s=Q5_EXPORT_LATENCY_MS / 1000.0,
    )
    base_latency_s = victim.round_duration_s
    round_inputs = []
    for round_id in range(n_rounds):
        active = round_id in active_rounds
        # Per-round interference magnitude jitter (lognormal-ish).
        magnitude_factor = max(0.0, rng.gauss(1.0, Q5_INTERFERENCE_CV))
        noise_s = rng.gauss(0.0, 0.001)
        latency_s = (
            base_latency_s
            + ((interference_delay_ms / 1000.0) * magnitude_factor if active else 0.0)
            + noise_s
        )
        detected_at = round_end_time(victim, round_id)
        round_inputs.append((round_id, detected_at, latency_s, active))
        if round_id in dropped_external:
            continue
        monitor.observe_round(
            round_id=round_id,
            detected_at=detected_at,
            victim_latency_s=latency_s,
            interferer_active=active,
        )

    predicted = {alert.round_id for alert in monitor.alerts}
    precision, recall = precision_recall_for_sets(predicted, active_rounds)
    ttds = []
    for start, end in episodes:
        detections = [
            alert for alert in monitor.alerts
            if start <= alert.round_id < end + window_size
        ]
        if not detections:
            continue
        first = min(detections, key=lambda alert: alert.detected_at)
        ttds.append(
            (first.detected_at - round_start_time(victim, start)) * 1000.0
        )

    external = base_row(
        workload="interference",
        query="Q5",
        system="sonata_external_windowed",
        seed=seed,
        job=victim,
        n_jobs=2,
    )
    external.update({
        "window_size": window_size,
        "interference_delay_ms": interference_delay_ms,
        "export_latency_ms": Q5_EXPORT_LATENCY_MS,
        "export_drop_prob": Q5_EXPORT_DROP_PROB,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "time_to_detection_ms": round(average_ttd_ms(ttds), 3),
        "state_entries": monitor.state_entries,
        "alerts": len(monitor.alerts),
    })

    inc_monitor = IncAwareMonitor(
        [victim, interferer],
        correlation_window=window_size,
        correlation_threshold=Q5_CORRELATION_THRESHOLD,
    )
    inc_corr = inc_monitor.correlation_monitor((victim.job_id, interferer.job_id))
    for round_id, detected_at, latency_s, active in round_inputs:
        inc_corr.observe_round(
            round_id=round_id,
            detected_at=detected_at,
            victim_latency_s=latency_s,
            interferer_active=active,
        )
    inc_predicted = {alert.round_id for alert in inc_corr.alerts}
    inc_precision, inc_recall = precision_recall_for_sets(inc_predicted, active_rounds)
    inc_ttds = []
    for start, end in episodes:
        detections = [
            alert for alert in inc_corr.alerts
            if start <= alert.round_id < end + window_size
        ]
        if not detections:
            continue
        first = min(detections, key=lambda alert: alert.detected_at)
        inc_ttds.append(
            (first.detected_at - round_start_time(victim, start)) * 1000.0
        )
    inc_aware = base_row(
        workload="interference",
        query="Q5",
        system="inc_aware",
        seed=seed,
        job=victim,
        n_jobs=2,
    )
    inc_aware.update({
        "window_size": window_size,
        "interference_delay_ms": interference_delay_ms,
        "export_latency_ms": 0.0,
        "export_drop_prob": 0.0,
        "precision": round(inc_precision, 3),
        "recall": round(inc_recall, 3),
        "time_to_detection_ms": round(average_ttd_ms(inc_ttds), 3),
        "state_entries": inc_corr.state_entries,
        "alerts": len(inc_corr.alerts),
    })

    rows = [external, inc_aware]
    for system, fn in (("sonata", q5_sonata), ("univmon", q5_univmon)):
        row = base_row(
            workload="interference",
            query="Q5",
            system=system,
            seed=seed,
            job=victim,
            n_jobs=2,
            expressible=False,
        )
        row.update({
            "window_size": window_size,
            "interference_delay_ms": interference_delay_ms,
            "precision": "",
            "recall": "",
            "time_to_detection_ms": "",
            "state_entries": "",
            "alerts": "",
        })
        try:
            fn([])
        except NotExpressible as exc:
            row["reason"] = str(exc)
        rows.append(row)

    return rows


# -----------------------------------------------------------------------------
# Summaries and output
# -----------------------------------------------------------------------------

def summarize_rows(rows: List[Dict]) -> List[Dict]:
    grouped: Dict[Tuple, List[Dict]] = defaultdict(list)
    for row in rows:
        key = tuple(row.get(field, "") for field in GROUP_FIELDS)
        grouped[key].append(row)

    summaries: List[Dict] = []
    for key, group in grouped.items():
        summary = {field: value for field, value in zip(GROUP_FIELDS, key)}
        summary["n_samples"] = len(group)
        summary["seed_min"] = min(int(row["seed"]) for row in group)
        summary["seed_max"] = max(int(row["seed"]) for row in group)
        for metric in METRIC_FIELDS:
            values = []
            for row in group:
                value = row.get(metric, "")
                if value == "" or value is None:
                    continue
                values.append(float(value))
            if not values:
                continue
            mean, low, high = mean_ci95(values)
            if metric in {"precision", "recall"}:
                low = max(0.0, low)
                high = min(1.0, high)
            summary[f"{metric}_mean"] = round(mean, 6)
            summary[f"{metric}_ci95_low"] = round(low, 6)
            summary[f"{metric}_ci95_high"] = round(high, 6)
        summaries.append(summary)

    return sorted(
        summaries,
        key=lambda r: (
            r.get("workload", ""),
            r.get("query", ""),
            r.get("system", ""),
            int(r.get("n_workers") or 0),
            float(r.get("delay_ms") or 0),
            float(r.get("loss_prob") or 0),
            r.get("fault_type", ""),
            float(r.get("timeout_ms") or 0),
            int(r.get("window_size") or 0),
            float(r.get("interference_delay_ms") or 0),
            r.get("key_mode", ""),
            r.get("control_type", ""),
            r.get("scaling_model", ""),
            int(r.get("n_jobs") or 0),
        ),
    )


def write_csv(path: str, rows: List[Dict]) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    keys = sorted({k for row in rows for k in row.keys()})
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def fmt_mean_ci(row: Dict, metric: str) -> str:
    mean = row.get(f"{metric}_mean")
    low = row.get(f"{metric}_ci95_low")
    high = row.get(f"{metric}_ci95_high")
    if mean is None:
        return "N/A"
    return f"{mean:.3f} [{low:.3f}, {high:.3f}]"


def print_compact_summary(summaries: List[Dict]) -> None:
    print()
    print("=" * 72)
    print("Confidence-interval summary (mean [95% CI])")
    print("=" * 72)
    for row in summaries:
        workload = row.get("workload")
        if workload not in {
            "straggler",
            "packet_loss",
            "multitenant",
            "round_failure",
            "interference",
            "univmon_key_sensitivity",
            "univmon_budget",
            "negative_control",
        }:
            continue
        setting = f"W={row.get('n_workers')}"
        if row.get("delay_ms") not in ("", None):
            setting += f" delay={row.get('delay_ms')}ms"
        if row.get("loss_prob") not in ("", None):
            setting += f" loss={row.get('loss_prob')}"
        if row.get("fault_type") not in ("", None):
            setting += f" fault={row.get('fault_type')}"
        if row.get("timeout_ms") not in ("", None):
            setting += f" timeout={row.get('timeout_ms')}ms"
        if row.get("window_size") not in ("", None):
            setting += f" window={row.get('window_size')}"
        if row.get("key_mode") not in ("", None):
            setting += f" key={row.get('key_mode')}"
        if row.get("sketch_width") not in ("", None):
            setting += f" width={row.get('sketch_width')}"
        if row.get("control_type") not in ("", None):
            setting += f" control={row.get('control_type')}"
        if row.get("n_jobs") not in ("", None):
            setting += f" jobs={row.get('n_jobs')}"
        print(
            f"  {workload:<12} {row.get('system'):<7} {setting:<30} "
            f"P={fmt_mean_ci(row, 'precision')} "
            f"R={fmt_mean_ci(row, 'recall')} "
            f"FPR={fmt_mean_ci(row, 'false_positive_rate')} "
            f"state={fmt_mean_ci(row, 'state_entries')} "
            f"bytes={fmt_mean_ci(row, 'register_bytes')} "
            f"TTD={fmt_mean_ci(row, 'time_to_detection_ms')}"
        )


# -----------------------------------------------------------------------------
# Main runner
# -----------------------------------------------------------------------------

def run_all(
    *,
    worker_counts: Iterable[int],
    job_counts: Iterable[int],
    n_chunks: int,
    n_rounds: int,
    seeds: Iterable[int],
) -> List[Dict]:
    rows: List[Dict] = []
    worker_counts = list(worker_counts)
    job_counts = list(job_counts)
    seed_list = list(seeds)

    print("=" * 72)
    print("Section 5.1: Methodology")
    print("=" * 72)
    print(f"  workers={worker_counts}")
    print(f"  n_chunks={n_chunks} ({n_chunks * 1024 / (1024 * 1024):.1f} MiB/job)")
    print(f"  n_rounds={n_rounds}")
    print(f"  n_jobs={job_counts}")
    print(f"  seeds={seed_list[0]}..{seed_list[-1]} ({len(seed_list)} seeds)")

    t0 = time.time()
    delays = [1, 10, 100]
    probs = [0.001, 0.01, 0.05]

    print()
    print("=" * 72)
    print("Section 5.2: Straggler detection (Q3)")
    print("=" * 72)
    for n_workers in worker_counts:
        job = JobConfig(
            job_id=0,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
        )
        straggler = min(7, n_workers - 1)
        for delay_ms in delays:
            before = len(rows)
            for seed in seed_list:
                rows.extend(
                    evaluate_q3_straggler(job, seed, delay_ms, straggler)
                )
            print(
                f"  W={n_workers:<2} delay={delay_ms:>3}ms "
                f"rows={len(rows) - before}"
            )

    print()
    print("=" * 72)
    print("Section 5.3: Packet-loss detection (Q2)")
    print("=" * 72)
    for n_workers in worker_counts:
        job = JobConfig(
            job_id=0,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
        )
        lossy_worker = min(7, n_workers - 1)
        for prob in probs:
            before = len(rows)
            for seed in seed_list:
                rows.extend(evaluate_q2_loss(job, seed, prob, lossy_worker))
            print(
                f"  W={n_workers:<2} loss={prob:<5} "
                f"rows={len(rows) - before}"
            )

    print()
    print("=" * 72)
    print("Section 5.3b: UnivMon key sensitivity (Q2)")
    print("=" * 72)
    for n_workers in worker_counts:
        job = JobConfig(
            job_id=0,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
        )
        lossy_worker = min(7, n_workers - 1)
        before = len(rows)
        for seed in seed_list:
            rows.extend(
                evaluate_univmon_key_sensitivity(
                    job, seed, loss_prob=0.01, lossy_worker=lossy_worker
                )
            )
        print(
            f"  W={n_workers:<2} key_modes={len(UNIVMON_KEY_MODES)} "
            f"rows={len(rows) - before}"
        )

    print()
    print("=" * 72)
    print("Section 5.3d: UnivMon sketch-budget sweep (Q2)")
    print("=" * 72)
    for n_workers in worker_counts:
        job = JobConfig(
            job_id=0,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
        )
        before = len(rows)
        for seed in seed_list:
            rows.extend(evaluate_univmon_sketch_budget(
                job, seed,
                loss_prob=UNIVMON_BUDGET_LOSS_PROB,
                widths=UNIVMON_BUDGET_WIDTHS,
            ))
        print(
            f"  W={n_workers:<2} widths={len(UNIVMON_BUDGET_WIDTHS)} "
            f"rows={len(rows) - before}"
        )

    print()
    print("=" * 72)
    print("Section 5.3c: Negative controls")
    print("=" * 72)
    for n_workers in worker_counts:
        job = JobConfig(
            job_id=0,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
        )
        before = len(rows)
        for seed in seed_list:
            rows.extend(evaluate_negative_controls(job, seed))
        print(f"  W={n_workers:<2} controls=6 rows={len(rows) - before}")

    print()
    print("=" * 72)
    print("Section 5.4: State cost under multi-tenancy")
    print("=" * 72)
    for n_workers in worker_counts:
        for n_jobs in job_counts:
            before = len(rows)
            for seed in seed_list:
                rows.extend(
                    evaluate_multitenant_state(
                        n_jobs=n_jobs,
                        n_workers=n_workers,
                        n_chunks=n_chunks,
                        n_rounds=n_rounds,
                        seed=seed,
                    )
                )
            print(
                f"  W={n_workers:<2} jobs={n_jobs:<2} "
                f"rows={len(rows) - before}"
            )

    print()
    print("=" * 72)
    print("Section 5.4b: Online round-failure detection (Q4) — timeout sweep")
    print("=" * 72)
    timeout_sweep_ms = (1.0, 5.0, 20.0, 50.0)
    for n_workers in worker_counts:
        job = JobConfig(
            job_id=0,
            n_workers=n_workers,
            n_chunks=n_chunks,
            n_rounds=n_rounds,
        )
        for fault_type in ("missing_chunks", "failed_round", "healthy_bursty"):
            for timeout_ms in timeout_sweep_ms:
                before = len(rows)
                for seed in seed_list:
                    rows.extend(evaluate_q4_round_failure(
                        job, seed, fault_type, timeout_ms=timeout_ms,
                    ))
                print(
                    f"  W={n_workers:<2} fault={fault_type:<14} "
                    f"timeout={timeout_ms:>4.0f}ms rows={len(rows) - before}"
                )

    print()
    print("=" * 72)
    print("Section 5.4c: Online cross-job interference detection (Q5)")
    print("=" * 72)
    for n_workers in worker_counts:
        before = len(rows)
        for seed in seed_list:
            rows.extend(
                evaluate_q5_interference(
                    n_workers=n_workers,
                    n_chunks=n_chunks,
                    n_rounds=n_rounds,
                    seed=seed,
                )
            )
        print(
            f"  W={n_workers:<2} window={Q5_WINDOW_SIZE:<2} "
            f"rows={len(rows) - before}"
        )

    print()
    print("=" * 72)
    print("Section 5.5: Inexpressibility audit")
    print("=" * 72)
    audit = {
        "Q1 (round latency)":    {"sonata": "OK",        "univmon": "NOT EXPR", "inc": "OK"},
        "Q2 (per-worker count)": {"sonata": "OK",        "univmon": "DEGRADED", "inc": "OK"},
        "Q3 (straggler ID)":     {"sonata": "OK (*)",    "univmon": "NOT EXPR", "inc": "OK"},
        "Q4 (round failure)":    {"sonata": "AWKWARD",   "univmon": "NOT EXPR", "inc": "OK"},
        "Q5 (cross-job corr.)":  {"sonata": "NOT EXPR",  "univmon": "NOT EXPR", "inc": "OK"},
    }
    print(f"  {'Query':<25} {'Sonata':<12} {'UnivMon':<12} {'INC-aware':<12}")
    print(f"  {'-' * 25} {'-' * 12} {'-' * 12} {'-' * 12}")
    for q, sysmap in audit.items():
        print(
            f"  {q:<25} {sysmap['sonata']:<12} "
            f"{sysmap['univmon']:<12} {sysmap['inc']:<12}"
        )
    print("  (*) Q3 uses a timestamp/worker argmax carried through reduce_by_key.")
    print(f"\nCompleted {len(rows)} per-seed rows in {time.time() - t0:.2f}s")

    annotate_hardware_costs(rows)
    return rows


def parse_int_list(value: str) -> List[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def resolve_config(args) -> Tuple[List[int], List[int], int, int, int]:
    if args.quick:
        workers = parse_int_list(args.workers) if args.workers else [16]
        jobs = parse_int_list(args.jobs) if args.jobs else [1, 2]
        n_chunks = args.n_chunks if args.n_chunks is not None else QUICK_N_CHUNKS
        n_rounds = args.n_rounds if args.n_rounds is not None else QUICK_N_ROUNDS
        seed_count = (
            args.seed_count if args.seed_count is not None else QUICK_SEED_COUNT
        )
    else:
        workers = (
            parse_int_list(args.workers)
            if args.workers else list(PAPER_WORKER_COUNTS)
        )
        jobs = parse_int_list(args.jobs) if args.jobs else list(PAPER_JOB_COUNTS)
        n_chunks = args.n_chunks if args.n_chunks is not None else PAPER_N_CHUNKS
        n_rounds = args.n_rounds if args.n_rounds is not None else PAPER_N_ROUNDS
        seed_count = (
            args.seed_count if args.seed_count is not None else DEFAULT_SEED_COUNT
        )
    return workers, jobs, n_chunks, n_rounds, seed_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true",
                        help="Use a small sweep for fast iteration")
    parser.add_argument("--workers", default=None,
                        help="Comma-separated worker counts")
    parser.add_argument("--jobs", default=None,
                        help="Comma-separated concurrent job counts")
    parser.add_argument("--n-chunks", type=int, default=None,
                        help="Chunks per worker per round")
    parser.add_argument("--n-rounds", type=int, default=None,
                        help="Rounds per job")
    parser.add_argument("--seed-count", type=int, default=None,
                        help="Number of seeds to run")
    parser.add_argument("--seed-start", type=int, default=0,
                        help="First seed value")
    parser.add_argument("--out", default="results/results.csv",
                        help="Path to write per-seed CSV results")
    parser.add_argument("--summary-out", default="results/summary.csv",
                        help="Path to write confidence-interval summary CSV")
    args = parser.parse_args()

    workers, jobs, n_chunks, n_rounds, seed_count = resolve_config(args)
    seeds = range(args.seed_start, args.seed_start + seed_count)

    rows = run_all(
        worker_counts=workers,
        job_counts=jobs,
        n_chunks=n_chunks,
        n_rounds=n_rounds,
        seeds=seeds,
    )
    summaries = summarize_rows(rows)
    write_csv(args.out, rows)
    write_csv(args.summary_out, summaries)
    print_compact_summary(summaries)
    print(f"\nWrote {len(rows)} per-seed rows to {args.out}")
    print(f"Wrote {len(summaries)} summary rows to {args.summary_out}")


if __name__ == "__main__":
    main()
