# The Observability Gap in In-Network Computing

Research paper for a networking/systems class. Conceptual + analytical + small empirical study.

## Thesis

Existing network telemetry systems (Sonata, UnivMon, INT, etc.) were designed around assumptions — flow-based traffic, heavy-hitter framing, per-packet stream processing — that in-network computing (INC) workloads systematically violate. The result is an observability gap: operators of INC systems cannot answer the monitoring questions that matter to them using current telemetry infrastructure.

## Research questions

- **RQ1**: What do operators of INC systems actually need to monitor? (Taxonomy)
- **RQ2**: For each monitoring need, can existing telemetry systems express the query, and with what accuracy/cost?
  - Part 1: analytical expressiveness
  - Part 2: empirical accuracy and resource cost
- **RQ3**: What primitives/designs would close the gap?

## Structural properties of INC traffic (drive everything)

1. **Equal-volume workers** — breaks heavy-hitter framing
2. **Synchronized rounds** — breaks flow-based assumptions
3. **Aggregate-event semantics** — breaks per-packet stream processing

## Five canonical INC monitoring queries (used throughout)

Map to taxonomy categories below. Used as the rows of the expressiveness matrix in §4 and as the workload of §5.

1. Aggregate-event monitoring (did this round complete? did aggregation succeed?)
2. Symmetric-participation monitoring (are all N expected contributors present?)
3. Tail-behavior monitoring (which worker is the straggler this round?)
4. Cross-job interference monitoring (is job A being slowed by job B?)
5. Application-semantic monitoring (rounds/sec, gradients/epoch)

Headline finding to preview in the abstract: of these five queries, sketch-based systems cleanly express zero and query-based systems cleanly express two.

## Paper outline (target lengths)

- Abstract — ~200 words
- §1 Introduction — ~1.5 pp. Open with motivating scenario (flaky-cable training-job example). Pivot to three structural differences. Numbered contributions mapped to RQs. Roadmap paragraph.
- §2 Background — ~2 pp
  - §2.1 In-network computing: ATP, SwitchML, NetLock, NetCache — focus on **traffic patterns**, not speedups
  - §2.2 Network telemetry: query-driven (Sonata, Marple), sketch-based (UnivMon, NitroSketch), in-band (INT, PINT) — abstraction + baked-in assumptions
- §3 The Observability Gap (RQ1) — ~2 pp. Derive taxonomy (5 categories above). For each: concrete example, why operator needs it, which structural property distinguishes it from canonical telemetry workloads. End with summary table (referenced throughout paper).
- §4 Expressiveness Analysis (RQ2 part 1) — ~2 pp. For Sonata and UnivMon, walk through all 5 queries. Either write the query in the system's primitives or explain why it can't be expressed. Use pseudo-code, not handwaving. Close with green/yellow/red table (queries × systems) — **visual centerpiece**.
- §5 Empirical Evaluation (RQ2 part 2) — ~3 pp
  - §5.1 Methodology (~½ pp): workload generator, simulated systems (Sonata-style dataflow, UnivMon-style sketch, oracle), queries, metrics (precision, recall, time-to-detection, state size)
  - §5.2 Accuracy under stragglers
  - §5.3 Accuracy under packet loss
  - §5.4 Cost under multi-tenancy (scaling vs. concurrent jobs)
  - §5.5 What was inexpressible (~½ pp) — often the most interesting part
- §6 Design Implications (RQ3) — ~2 pp. **Sketch design space, don't propose a complete system.**
  - §6.1 Primitives the gap demands: aggregate events, expected-set tracking, cross-stream correlation
  - §6.2 Resource sharing with the INC application (separate pipelines / shared state / sampling / SmartNIC offload)
  - §6.3 Application-aware vs. application-agnostic telemetry tradeoff
- §7 Limitations and Threats to Validity — ~½ pp. Be honest: Sonata/UnivMon impls are abstractions, workload is synthetic, taxonomy derived from a handful of INC systems, ATP is one point in a broader design space.
- §8 Related Work — ~1 pp
  - Telemetry systems: Sonata, UnivMon, Marple, NitroSketch, PINT, INT — none specifically targets INC
  - INC systems: ATP, SwitchML, NetLock, NetCache — their eval sections are bespoke telemetry, no general framework
  - Application-aware monitoring: dShark, Confluo, Trumpet, AI-training observability — closest prior art, distinguish carefully
- §9 Conclusion — ~½ pp. Recap three findings. Broader point: as INC matures from research to production, the observability gap becomes a deployment blocker; telemetry community needs to treat INC traffic as first-class.
- References — 25–40 citations. ~12 from syllabus, rest from related-work landscape.

## Key systems referenced

- **INC**: ATP, SwitchML, NetLock, NetCache
- **Telemetry (query-driven)**: Sonata, Marple
- **Telemetry (sketch-based)**: UnivMon, NitroSketch
- **Telemetry (in-band)**: INT, PINT
- **Application-aware monitoring** (prior art to distinguish from): dShark, Confluo, Trumpet

## How to help on this paper

- Default deliverable for any section is prose that fits the target length above — don't over-produce.
- When drafting §4 query analysis: pseudo-code in the system's actual primitives, not English paraphrase.
- The expressiveness table in §4 and the taxonomy table in §3 are the two artifacts the rest of the paper hangs off — treat them as load-bearing.
- §5's simulated systems are intentionally abstractions of Sonata/UnivMon, not faithful reimplementations; flag this in §7 not §5.
- §6 is a design-space discussion, not a system proposal. If drafts start sounding like "we propose X", pull back.
- Preserve the structural-differences framing (equal-volume / synchronized rounds / aggregate-event semantics) — it's the conceptual spine.

## Companion code (this directory)

A small Python harness implements the §5 experiments. README.md has the user-facing summary; this section is the orientation for editing the code.

### Files

| File | Role | Paper §
|---|---|---|
| `workload.py` | ATP-style packet generator; `JobConfig`, `AnomalyConfig`, `scenario_*` builders | §5.1 |
| `systems.py` | `Oracle`, Sonata-style `Pipeline`, online round-timeout monitor, windowed correlation monitor, INC-aware strawman monitor, `UnivMonSketch` | §2, §5.1 |
| `queries.py` | Q1–Q5 implementations per system; `NotExpressible` raised with structural reason for inexpressible cases | §4 |
| `evaluate.py` | Runs scenarios, computes precision/recall/state cost, writes `results/results.csv`, prints §5.5 audit | §5 |
| `plots.py` | Reads CSV, writes Section 5 PDFs into `results/figures/` | §5.2–5.4 |

### Running

```bash
python3 evaluate.py            # paper-methodology sweep, writes CSVs
python3 evaluate.py --quick    # smaller smoke sweep
python3 evaluate.py --seed-count 50
python3 plots.py               # writes results/figures/*.pdf
```

Outputs land under `results/` (gitignored if/when a repo is initialized).
`results/results.csv` is per-seed output; `results/summary.csv` reports
means and 95% confidence intervals, including `time_to_detection_ms` for
online Q4/Q5 monitors. Resource columns include `counter_bits`,
`register_bytes`, `keys_tracked`, `per_packet_ops`, and `scaling_model`.
Inexpressible rows use `expressible=False`, blank numeric accuracy/resource
fields, and a structural `reason`; plots annotate those rows as N/A.
Rows with `system=inc_aware` are a reference strawman, not a proposed full
system: they give the monitor expected workers/chunks, bounded round state,
timeouts, and job-aware correlation so the experiment can compare current
query models against the primitive the paper argues is missing.
Verified working with Python 3.11 + matplotlib 3.8.

### Expected result shape

Re-run before citing exact numbers in §5.

- **§5.1 methodology** — default sweep uses workers {8,16,32},
  25MiB gradients (25,600 1KiB chunks), 50 rounds, jobs {1,2,4}, and
  20 seeds.
- **§5.2 stragglers (Q3)** — Sonata should remain P=R=1.0 at
  1/10/100 ms delay. UnivMon is structurally inexpressible and appears
  as N/A, not as zero precision/recall.
- **§5.3 packet loss (Q2)** — Sonata remains P=R=1.0 across loss
  probabilities (0.1%/1%/5%) at paper scale. UnivMon is degraded;
  recall falls with worker cardinality, not loss rate, because sketch
  collisions dominate small per-worker count gaps. At default width=512,
  recall is ~0.50 at N=8, ~0.14 at N=16, ~0.05 at N=32. At N=32 the
  sketch also loses precision (~0.55–0.65) — sketch overestimates start
  alerting workers that did not actually drop a chunk. To match Sonata's
  recall at N=32 the sketch needs ~32k registers (~655 KB), roughly
  100× Sonata's per-key store; report this in §5.3 as the budget result.
- **§5.3b UnivMon key sensitivity (Q2)** — intermittent loss is used
  so `(worker)` and `(job, worker)` keys lose the bad-round dimension.
  `(job, round, worker)` is the natural but collision-prone key. The
  `(job, round, worker, chunk)` key is the high-cardinality escape hatch:
  it can recover the answer only by making chunks part of the flow key
  and enumerating the expected universe.
- **§5.3c negative controls** — rows report `false_positive_rate` for
  no anomaly, benign jitter, staggered job starts, and background traffic.
  Precision/recall are intentionally blank because there are no true
  positives in these controls. Q2 (`sonata_control`) and Q3
  (`sonata_control`) hold FPR=0 across baseline/jitter/background. Q5
  (`sonata_external_windowed`) holds FPR=0 on staggered_starts and ~0.01
  on background_traffic — call out the residual rate honestly in prose;
  it is the correlation monitor's tolerance, not a Q5 expressibility
  failure.
- **§5.4 multi-tenancy (Q2)** — Sonata state grows linearly with
  jobs × rounds × workers (1,600 B at {1 job, 8 workers} → 25,600 B at
  {4 jobs, 32 workers}); UnivMon stays fixed at ~10,240 B for the default
  sketch geometry. This is the one regime where the sketch's resource
  model favors it; flag this honestly in §5.4 prose, and note that the
  Sonata advantage on Q2 accuracy (§5.3) costs proportionally more state
  here. The plot uses approximate register bytes, not just logical state
  entries.
- **§5.4b round failure (Q4)** — `sonata_windowed` uses a configured
  expected-round schedule and timeout. At timeout ≥ 5 ms it detects
  missing chunks and fully failed rounds with P=R=1.0; at timeout=1 ms
  precision drops to ~0.17 (recall stays 1.0) because the monitor fires
  before legitimate rounds finish — bias/variance knob behaving as
  designed. Time-to-detection equals the timeout on `failed_round` and
  ≈110 ms at 50 ms timeout for partial `missing_chunks` (timer has to
  elapse before the gap is declarable). `healthy_bursty` yields zero
  alerts at all timeouts. `sonata_windowed.state_entries` scales as
  workers × chunks (~204,800 at N=8 up to ~819,200 at N=32) for the
  expected set; `inc_aware` is the strawman expected-set/round-state
  baseline and reaches the same P=R behaviour with ~25,600 entries
  (single round of chunks), an 8–32× state ratio that is the §6.1
  expected-set primitive argument. UnivMon remains structurally
  inexpressible and appears as N/A.
- **§5.4c interference (Q5)** — `sonata_external_windowed` models
  Sonata-style per-round latency output feeding a downstream sliding
  correlation monitor. Native Sonata and UnivMon remain N/A. At the
  default 20 ms induced delay and window=8 the job-aware `inc_aware`
  baseline reaches P≈0.48 / R≈0.85 with time-to-detection ≈141 ms;
  `sonata_external_windowed` reaches P≈0.43 / R≈0.67 with
  time-to-detection ≈304 ms. Sub-0.5 precision is the genuine fuzziness
  of correlation-based interference detection — discuss as a §6 design
  limitation, not a workload bug. Exact numbers shift with window and
  threshold; re-run before citing.
- **§5.5 audit** — Sonata expresses Q1/Q2/Q3 cleanly (Q2/Q3 hit P=R=1.0 in
  the sweep), Q4 awkwardly (only via the `sonata_windowed` variant with
  an expected-set primitive baked in), Q5 not at all natively (only via
  the `sonata_external_windowed` downstream-correlation variant). UnivMon
  expresses zero of the five cleanly; Q2 is "degraded" (recall collapses
  with N, precision collapses at high N), the rest are NotExpressible
  and surface as `expressible=False` with a structural reason. `inc_aware`
  covers Q1–Q5 because it is given the missing INC primitives, and is
  the load-bearing point of comparison for §6.

### Conventions for editing the code

- Inexpressibility is a first-class result. When a query can't be expressed, raise `NotExpressible` with a one-sentence structural reason — the docstring/exception message becomes §4/§5.5 prose. Don't paper over it with a degraded heuristic unless explicitly modeling "degraded" (Q2 UnivMon).
- All workloads are deterministic given a seed. Keep them that way;
  figures should not change run-to-run except when seed ranges or
  methodology parameters change.
- The `state_entries` accounting on each operator is what §5.4 reports; if you add operators or sketch structures, make sure they expose this property.
- `UnivMonSketch.key_fn` is a deliberate design knob — switching the flow key changes which queries become askable. Discuss the tradeoff in §6.1 (cross-stream correlation primitive), not §5.
- Sonata/UnivMon impls are abstractions; if a reviewer-style question arises about fidelity, the answer goes in §7 (limitations), not in the code.
