"""
Synthetic in-network computing workload generator.

Models ATP-style aggregation traffic: N workers send equal-sized gradient
chunks to a switch in synchronized rounds. The switch aggregates and emits
one combined chunk per chunk_id, then the next round begins after a
configurable inter-round gap.

Supports anomaly injection (stragglers, packet loss, asymmetric
participation, round failure) and multiple concurrent jobs for
multi-tenancy experiments.
"""

from dataclasses import dataclass, field
from typing import Iterator, List, Dict, Set, Tuple, Optional
import random
import heapq
import math


# -----------------------------------------------------------------------------
# Paper-scale methodology constants
# -----------------------------------------------------------------------------

PAPER_WORKER_COUNTS = [8, 16, 32]
PAPER_JOB_COUNTS = [1, 2, 4]
PAPER_PACKET_BYTES = 1024
PAPER_GRADIENT_BYTES = 25 * 1024 * 1024
PAPER_N_CHUNKS = PAPER_GRADIENT_BYTES // PAPER_PACKET_BYTES
PAPER_N_ROUNDS = 50


# -----------------------------------------------------------------------------
# Packet model
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Packet:
    """A single ATP-style aggregation packet.

    The header fields below are the ones a telemetry system gets to filter
    and group on. In a real implementation they would be parsed out of an
    ATP header by a P4 program; here we materialize them directly.
    """
    timestamp: float   # seconds since experiment start
    job_id: int        # which training job this belongs to
    round_id: int      # which aggregation round within the job
    worker_id: int     # which worker sent it
    chunk_id: int      # which gradient chunk within the round
    size: int = 1024   # bytes


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

@dataclass
class JobConfig:
    """Parameters for a single training job.

    Defaults are chosen for tractable simulation, not for fidelity to real
    ML training. A realistic job has gradient_bytes ~ 25 MB (i.e.,
    ~25k chunks per round); the n_chunks default below is two orders of
    magnitude smaller. The structural conclusions about telemetry
    expressiveness are independent of this scale.
    """
    job_id: int
    n_workers: int = 16
    n_chunks: int = 64
    packet_bytes: int = 1024
    n_rounds: int = 20
    round_duration_s: float = 0.1     # aggregation phase
    inter_round_gap_s: float = 0.05   # quiet (compute) phase
    start_offset_s: float = 0.0       # when this job's first round begins


@dataclass
class AnomalyConfig:
    """Anomalies to inject for one job.

    Each field is keyed by worker_id (within the job) or by
    (job_id, round_id) for failed_rounds.
    """
    # worker_id -> additional delay in seconds applied to every packet
    straggler_workers: Dict[int, float] = field(default_factory=dict)
    # worker_id -> independent per-packet drop probability
    lossy_workers: Dict[int, float] = field(default_factory=dict)
    # worker_id -> set of chunk_ids the worker skips entirely
    skipping_workers: Dict[int, Set[int]] = field(default_factory=dict)
    # (job_id, round_id) tuples that are dropped wholesale
    failed_rounds: Set[Tuple[int, int]] = field(default_factory=set)
    # uniform per-packet drop probability applied to every worker, on top of
    # any worker-specific lossy_workers entry. Models background loss so the
    # lossy worker is not trivially the only worker missing packets.
    baseline_loss_prob: float = 0.0


def round_start_time(job: JobConfig, round_id: int) -> float:
    """Expected start time for a job round."""
    return (
        job.start_offset_s
        + round_id * (job.round_duration_s + job.inter_round_gap_s)
    )


def round_end_time(job: JobConfig, round_id: int) -> float:
    """Expected end time for a job aggregation round."""
    return round_start_time(job, round_id) + job.round_duration_s


# -----------------------------------------------------------------------------
# Generation
# -----------------------------------------------------------------------------

def generate_job(
    job: JobConfig,
    anomaly: AnomalyConfig,
    seed: int = 0,
) -> Iterator[Packet]:
    """Yield packets for one job in timestamp order.

    Packet layout within a round: each chunk_id occupies a slice of the
    round duration; within a slice, all workers send concurrently with
    small per-packet jitter. Straggler delay is added on top of the base
    timestamp.
    """
    rng = random.Random(seed)

    for round_id in range(job.n_rounds):
        if (job.job_id, round_id) in anomaly.failed_rounds:
            continue

        round_start = round_start_time(job, round_id)

        for chunk_id in range(job.n_chunks):
            chunk_base_t = (
                round_start
                + (chunk_id / job.n_chunks) * job.round_duration_s
            )
            for worker_id in range(job.n_workers):
                if chunk_id in anomaly.skipping_workers.get(worker_id, set()):
                    continue
                if rng.random() < anomaly.lossy_workers.get(worker_id, 0.0):
                    continue

                # Small per-packet jitter so simultaneous worker sends
                # don't pile onto exactly the same timestamp.
                jitter = rng.uniform(0.0, 0.0001)
                delay = anomaly.straggler_workers.get(worker_id, 0.0)
                t = chunk_base_t + jitter + delay

                yield Packet(
                    timestamp=t,
                    job_id=job.job_id,
                    round_id=round_id,
                    worker_id=worker_id,
                    chunk_id=chunk_id,
                    size=job.packet_bytes,
                )


def generate_workload(
    jobs: List[JobConfig],
    anomalies: Optional[Dict[int, AnomalyConfig]] = None,
    seed: int = 0,
) -> Iterator[Packet]:
    """Merge packet streams from multiple jobs in timestamp order.

    `anomalies` maps job_id to AnomalyConfig; missing entries default to
    no anomalies.
    """
    anomalies = anomalies or {}
    streams = [
        generate_job(
            job,
            anomalies.get(job.job_id, AnomalyConfig()),
            seed=seed + job.job_id,
        )
        for job in jobs
    ]
    yield from heapq.merge(*streams, key=lambda p: p.timestamp)


def generate_round_tail_packets(
    job: JobConfig,
    anomaly: AnomalyConfig,
    seed: int = 0,
) -> Iterator[Packet]:
    """Yield the final packet per worker per round.

    Q3 (straggler identification) depends only on the maximum timestamp per
    (job, round), so this compact stream is equivalent to the full packet
    stream for Q3 while avoiding paper-scale packet materialization.
    """
    rng = random.Random(seed)
    last_chunk = max(0, job.n_chunks - 1)

    for round_id in range(job.n_rounds):
        if (job.job_id, round_id) in anomaly.failed_rounds:
            continue

        round_start = round_start_time(job, round_id)
        chunk_base_t = (
            round_start
            + (last_chunk / job.n_chunks) * job.round_duration_s
        )
        for worker_id in range(job.n_workers):
            if last_chunk in anomaly.skipping_workers.get(worker_id, set()):
                continue
            if rng.random() < anomaly.lossy_workers.get(worker_id, 0.0):
                continue

            jitter = rng.uniform(0.0, 0.0001)
            delay = anomaly.straggler_workers.get(worker_id, 0.0)
            yield Packet(
                timestamp=chunk_base_t + jitter + delay,
                job_id=job.job_id,
                round_id=round_id,
                worker_id=worker_id,
                chunk_id=last_chunk,
                size=job.packet_bytes,
            )


def _sample_approx_binomial(n: int, p: float, rng: random.Random) -> int:
    """Sample dropped packets without looping over all chunks at paper scale."""
    if p <= 0.0:
        return 0
    if p >= 1.0:
        return n
    if n <= 4096:
        return sum(1 for _ in range(n) if rng.random() < p)

    mean = n * p
    sd = math.sqrt(n * p * (1.0 - p))
    sample = int(round(rng.gauss(mean, sd)))
    return max(0, min(n, sample))


def generate_worker_round_counts(
    job: JobConfig,
    anomaly: AnomalyConfig,
    seed: int = 0,
) -> Dict[Tuple[int, int, int], int]:
    """Return packet counts per (job, round, worker).

    Q2 depends only on per-worker contribution counts, so this avoids
    generating every chunk packet when n_chunks is set to a 25MB gradient.
    """
    rng = random.Random(seed)
    counts: Dict[Tuple[int, int, int], int] = {}
    baseline_p = max(0.0, anomaly.baseline_loss_prob)

    for round_id in range(job.n_rounds):
        if (job.job_id, round_id) in anomaly.failed_rounds:
            continue
        for worker_id in range(job.n_workers):
            skipped = len(anomaly.skipping_workers.get(worker_id, set()))
            possible = max(0, job.n_chunks - skipped)
            worker_p = anomaly.lossy_workers.get(worker_id, 0.0)
            # Compound two independent loss processes (worker-specific +
            # baseline) into an effective per-packet drop probability.
            eff_p = 1.0 - (1.0 - worker_p) * (1.0 - baseline_p)
            dropped = _sample_approx_binomial(possible, eff_p, rng)
            counts[(job.job_id, round_id, worker_id)] = possible - dropped

    return counts


# -----------------------------------------------------------------------------
# Convenience scenario builders
# -----------------------------------------------------------------------------

def scenario_baseline(
    seed: int = 0,
    *,
    n_workers: int = 16,
    n_chunks: int = 64,
    n_rounds: int = 20,
) -> Tuple[List[JobConfig], List[Packet]]:
    """Single job, no anomalies. Sanity check."""
    job = JobConfig(
        job_id=0,
        n_workers=n_workers,
        n_chunks=n_chunks,
        n_rounds=n_rounds,
    )
    return [job], list(generate_workload([job], seed=seed))


def scenario_straggler(
    delay_ms: float,
    straggler_worker: int = 7,
    seed: int = 0,
    *,
    n_workers: int = 16,
    n_chunks: int = 64,
    n_rounds: int = 20,
) -> Tuple[List[JobConfig], List[Packet]]:
    """Single job; one worker is consistently slow."""
    job = JobConfig(
        job_id=0,
        n_workers=n_workers,
        n_chunks=n_chunks,
        n_rounds=n_rounds,
    )
    a = AnomalyConfig(straggler_workers={straggler_worker: delay_ms / 1000.0})
    return [job], list(generate_workload([job], {0: a}, seed=seed))


def scenario_packet_loss(
    loss_prob: float,
    lossy_worker: int = 7,
    seed: int = 0,
    *,
    n_workers: int = 16,
    n_chunks: int = 64,
    n_rounds: int = 20,
) -> Tuple[List[JobConfig], List[Packet]]:
    """Single job; one worker drops packets at given probability."""
    job = JobConfig(
        job_id=0,
        n_workers=n_workers,
        n_chunks=n_chunks,
        n_rounds=n_rounds,
    )
    a = AnomalyConfig(lossy_workers={lossy_worker: loss_prob})
    return [job], list(generate_workload([job], {0: a}, seed=seed))


def scenario_multitenant(
    n_jobs: int,
    seed: int = 0,
    *,
    n_workers: int = 16,
    n_chunks: int = 64,
    n_rounds: int = 20,
) -> Tuple[List[JobConfig], List[Packet]]:
    """Multiple concurrent jobs with slight staggered starts."""
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
    return jobs, list(generate_workload(jobs, seed=seed))
