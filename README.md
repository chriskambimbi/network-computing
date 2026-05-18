# INC Telemetry Experiment

Companion code for **The Observability Gap in In-Network Computing**.

## What this is

A small Python harness that:

1. Generates synthetic ATP-style aggregation traffic with controllable
   anomalies (stragglers, packet loss, asymmetric participation,
   round failure, multi-tenant interference).
2. Implements four telemetry systems/baselines: an Oracle (full
   visibility), a Sonata-style dataflow engine, a UnivMon-style
   Count-Sketch hierarchy, and a deliberately simple INC-aware strawman
   monitor with expected-set tracking and per-round state.
3. Executes the five canonical queries (Q1–Q5) from the paper in each
   system where they are expressible, and records why they are not
   expressible elsewhere.
4. Computes precision, recall, and state cost; writes CSV outputs and
   produces the Section 5 figures.

The implementations are abstractions, not the real systems. We are
evaluating the expressiveness of their query models, not their
end-to-end performance — see the Limitations section of the paper.

## Files

| File | Purpose | Paper section |
|---|---|---|
| `workload.py` | ATP-style traffic generator + anomaly injection | §5.1 |
| `systems.py` | Oracle, Sonata pipeline, online/windowed monitors, INC-aware strawman, UnivMon sketch | §2, §5.1 |
| `queries.py` | Q1–Q5 implementations across systems | §4 |
| `evaluate.py` | Main runner; produces `results/results.csv` | §5 |
| `plots.py` | Generates PDFs in `results/figures/` | §5 |

## Requirements

- Python 3.9+
- `matplotlib` (only for plots; everything else is stdlib)

```
pip install -r requirements.txt
```

## Running

```bash
# Paper-methodology run: workers={8,16,32}, 25MiB gradients,
# 50 rounds, jobs={1,2,4}, 20 seeds by default.
python evaluate.py

# Quick smoke test with reduced chunks/rounds and 2 seeds
python evaluate.py --quick

# More confidence-interval samples
python evaluate.py --seed-count 50

# Generate figures
python plots.py
```

After running, you'll have:

```
results/
├── results.csv
├── summary.csv
└── figures/
    ├── straggler_accuracy.pdf
    ├── packet_loss_accuracy.pdf
    ├── multitenant_state.pdf
    ├── round_failure_online.pdf
    ├── interference_online.pdf
    ├── univmon_key_sensitivity.pdf
    └── negative_controls.pdf
```

## Mapping to the paper

The console output is organized to mirror Section 5 of the paper:

- **§5.1** Methodology / baseline sanity check
- **§5.2** Straggler-detection accuracy across delay magnitudes
- **§5.3** Packet-loss detection across loss probabilities
- **§5.3b** UnivMon Q2 sensitivity across flow keys: worker,
  job-worker, job-round-worker, and job-round-worker-chunk
- **§5.3c** Negative controls: no anomaly, benign jitter, staggered
  job starts, and unrelated background traffic
- **§5.4** State cost under multi-tenancy
- **§5.4b** Online Q4 round-failure detection with missing chunks and
  failed rounds
- **§5.4c** Online Q5 cross-job interference detection with a sliding
  correlation window
- **§5.5** Inexpressibility audit (qualitative table), including the
  INC-aware strawman as a reference for the missing primitives

`results.csv` contains one row per seed and includes the experiment
parameters (`seed`, `n_workers`, `n_chunks`, `n_rounds`, `n_jobs`).
`summary.csv` groups those rows and reports means plus 95% confidence
intervals, including `time_to_detection_ms` for online Q4/Q5 monitors.
By default `plots.py` uses `summary.csv` when it is present.

State cost is reported both as logical `state_entries` and as hardware-
oriented approximations: `counter_bits`, `register_bytes`,
`keys_tracked`, `per_packet_ops`, and `scaling_model`. Rows where a
query is structurally inexpressible use `expressible=False`, leave
numeric accuracy/resource fields blank, and carry a `reason`; the plots
annotate these as N/A instead of drawing them as zero-accuracy results.
Negative-control rows report `false_positive_rate` instead of
precision/recall because there are no true anomalies.

`system=inc_aware` is a strawman reference baseline rather than a full
proposal. It gives the monitor the primitive the paper argues current
systems lack: knowledge of the expected workers/chunks for each round,
bounded active-round state, timeout-based aggregate-event detection, and
job-aware round-latency correlation. Its purpose is to show what becomes
straightforward once INC semantics are first-class, while making the
state scaling visible in the same CSV and figures as Sonata/UnivMon.

## Extending

To add a new query: write a `qN_oracle`, `qN_sonata`, and `qN_univmon`
in `queries.py`, then wire it into `evaluate.py`. If the query is
inexpressible in a system, raise `NotExpressible` with a comment
explaining the structural reason — that comment will become part of
the paper's analysis.

To add a new anomaly: extend `AnomalyConfig` in `workload.py` and the
generator loop in `generate_job`. Then add a `scenario_*` builder
function for convenience.

## Notes

- All experiments are deterministic given a seed. Default seed = 0.
- The paper-methodology run uses 25MiB gradients, represented as
  25,600 1KiB chunks per worker per round. Q2 and Q3 use compact
  count/tail generators so the experiment records paper-scale packet
  counts without materializing every packet object in memory.
- Q4 and Q5 are online/windowed experiments. Q4 uses a configured
  expected-round schedule and emits on timeout; Q5 uses Sonata-style
  per-round latency output plus an external sliding-window correlator.
  The INC-aware strawman runs the same online tasks with expected-set
  and round-state primitives. Native Sonata/UnivMon inexpressibility is
  still recorded as N/A rows.
- UnivMon key sensitivity uses intermittent packet loss so coarse keys
  that omit `round_id` cannot recover which rounds were bad. The chunk
  key is modeled as the high-cardinality escape hatch: it can recover the
  answer only by making chunks part of the flow key and enumerating the
  expected key universe.
- Negative controls are explicitly false-positive tests. They should not
  be interpreted as accuracy experiments because the actual-positive set
  is empty by construction.
- Hardware-cost fields are approximate query-model estimates, not
  device-specific ASIC placement results. They are intended to support
  the paper's resource-sharing discussion.
- The Count-Sketch implementation uses MD5 for hashing — fine for
  evaluation, but in a production sketch you would want a faster
  pairwise-independent hash family.
