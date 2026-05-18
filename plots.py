"""
Generate paper figures from results.csv or summary.csv.

Produces:
  results/figures/straggler_accuracy.pdf   --- Section 5.2
  results/figures/packet_loss_accuracy.pdf --- Section 5.3
  results/figures/multitenant_state.pdf    --- Section 5.4
  results/figures/round_failure_online.pdf --- Q4 online timeout detection
  results/figures/interference_online.pdf   --- Q5 windowed correlation
  results/figures/univmon_key_sensitivity.pdf --- Q2 key-mode sensitivity
  results/figures/negative_controls.pdf     --- false-positive controls

Usage:
    python plots.py
    python plots.py --in results/summary.csv
"""

import argparse
import csv
import os
from collections import defaultdict

import math

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


PLOT_STYLE = {
    "figure.figsize": (5.0, 3.0),
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "legend.fontsize": 8,
    "lines.linewidth": 1.6,
    "lines.markersize": 5,
}


def load_rows(path: str):
    with open(path) as f:
        return list(csv.DictReader(f))


def fnum(x):
    """Float-or-none."""
    if x is None or x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def is_false(value) -> bool:
    return value is False or str(value).lower() == "false"


def inexpressible_note(rows, workload: str) -> str:
    systems = sorted({
        r.get("system", "")
        for r in rows
        if r.get("workload") == workload and is_false(r.get("expressible"))
    })
    systems = [s for s in systems if s]
    if not systems:
        return ""
    return "N/A: " + ", ".join(systems) + " not expressible"


def add_inexpressible_note(ax, rows, workload: str) -> None:
    note = inexpressible_note(rows, workload)
    if not note:
        return
    ax.text(
        0.02, 0.04, note,
        transform=ax.transAxes,
        fontsize=7,
        va="bottom",
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "0.75"},
    )


def mean_ci95(values):
    mean = sum(values) / len(values)
    if len(values) == 1:
        return mean, mean, mean
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    half_width = 1.96 * math.sqrt(var) / math.sqrt(len(values))
    return mean, mean - half_width, mean + half_width


def collect_points(rows, workload: str, x_field: str, metric: str):
    """Return points grouped by (system, n_workers), with CI bounds."""
    metric_mean = f"{metric}_mean"
    metric_low = f"{metric}_ci95_low"
    metric_high = f"{metric}_ci95_high"
    has_summary = any(metric_mean in r for r in rows)

    if has_summary:
        grouped = defaultdict(list)
        for r in rows:
            if r.get("workload") != workload:
                continue
            x = fnum(r.get(x_field))
            y = fnum(r.get(metric_mean))
            if None in (x, y):
                continue
            low = fnum(r.get(metric_low))
            high = fnum(r.get(metric_high))
            if low is None or high is None:
                low = high = y
            workers = int(fnum(r.get("n_workers")) or 0)
            grouped[(r["system"], workers)].append((x, y, low, high))
        return grouped

    raw = defaultdict(list)
    for r in rows:
        if r.get("workload") != workload:
            continue
        x = fnum(r.get(x_field))
        y = fnum(r.get(metric))
        if None in (x, y):
            continue
        workers = int(fnum(r.get("n_workers")) or 0)
        raw[(r["system"], workers, x)].append(y)

    grouped = defaultdict(list)
    for (system, workers, x), values in raw.items():
        mean, low, high = mean_ci95(values)
        if metric in {"precision", "recall"}:
            low = max(0.0, low)
            high = min(1.0, high)
        grouped[(system, workers)].append((x, mean, low, high))
    return grouped


def plot_metric_pair(rows, workload: str, x_field: str, xlabel: str,
                     title: str, outpath: str, log_x: bool = False):
    precision = collect_points(rows, workload, x_field, "precision")
    recall = collect_points(rows, workload, x_field, "recall")
    keys = sorted(set(precision) | set(recall))
    if not keys:
        print(f"  [skip] no {workload} rows")
        return

    fig, ax = plt.subplots()
    for key in keys:
        system, workers = key
        label_prefix = f"{system} W={workers}" if workers else system
        for metric_name, points, marker, linestyle in (
            ("precision", precision.get(key, []), "o", "-"),
            ("recall", recall.get(key, []), "s", "--"),
        ):
            if not points:
                continue
            points.sort()
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            yerr = [
                [max(0.0, p[1] - p[2]) for p in points],
                [max(0.0, p[3] - p[1]) for p in points],
            ]
            ax.errorbar(
                xs, ys, yerr=yerr, marker=marker, linestyle=linestyle,
                capsize=2, label=f"{label_prefix} ({metric_name})",
            )
    if log_x:
        ax.set_xscale("log")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("Precision / Recall")
    ax.set_ylim(-0.05, 1.05)
    ax.set_title(title)
    ax.legend(loc="best", ncol=2)
    ax.grid(True, alpha=0.3)
    add_inexpressible_note(ax, rows, workload)
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_straggler(rows, outpath: str):
    """Precision/recall of straggler detection vs straggler delay."""
    plot_metric_pair(
        rows, "straggler", "delay_ms", "Straggler delay (ms)",
        "Straggler detection vs delay magnitude", outpath, log_x=True,
    )


def plot_packet_loss(rows, outpath: str):
    """Precision/recall of packet-loss detection vs loss probability."""
    plot_metric_pair(
        rows, "packet_loss", "loss_prob", "Per-packet loss probability",
        "Lossy-worker detection vs loss probability", outpath, log_x=True,
    )


def plot_multitenant(rows, outpath: str):
    """State cost vs number of concurrent jobs."""
    by_system = collect_points(rows, "multitenant", "n_jobs", "register_bytes")
    ylabel = "Approx. register bytes"
    if not by_system:
        by_system = collect_points(rows, "multitenant", "n_jobs", "state_entries")
        ylabel = "State entries"
    if not by_system:
        print("  [skip] no multitenant rows")
        return

    fig, ax = plt.subplots()
    for (system, workers), pts in sorted(by_system.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        yerr = [
            [max(0.0, p[1] - p[2]) for p in pts],
            [max(0.0, p[3] - p[1]) for p in pts],
        ]
        label = f"{system} W={workers}" if workers else system
        ax.errorbar(xs, ys, yerr=yerr, marker="o", capsize=2, label=label)
    ax.set_xlabel("Concurrent jobs")
    ax.set_ylabel(ylabel)
    ax.set_title("Hardware state cost under multi-tenancy")
    ax.legend(ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_online_rows(rows, workload: str, system: str):
    selected = []
    for r in rows:
        if r.get("workload") != workload or r.get("system") != system:
            continue
        n_workers = fnum(r.get("n_workers"))
        precision = fnum(r.get("precision_mean"))
        recall = fnum(r.get("recall_mean"))
        ttd = fnum(r.get("time_to_detection_ms_mean"))
        if None in (n_workers, precision, recall, ttd):
            continue
        selected.append((int(n_workers), precision, recall, ttd, r))
    return sorted(
        selected,
        key=lambda item: (item[0], item[4].get("fault_type", "")),
    )


def plot_round_failure(rows, outpath: str):
    """Q4 online failure precision/recall and detection latency."""
    selected = (
        plot_online_rows(rows, "round_failure", "sonata_windowed")
        + plot_online_rows(rows, "round_failure", "inc_aware")
    )
    if not selected:
        print("  [skip] no round_failure rows")
        return

    by_fault = defaultdict(list)
    for n_workers, precision, recall, ttd, row in selected:
        label = f"{row.get('system')} {row.get('fault_type', 'unknown')}"
        by_fault[label].append(
            (n_workers, precision, recall, ttd)
        )

    fig, (ax_acc, ax_ttd) = plt.subplots(1, 2, figsize=(7.0, 3.0))
    for fault_type, pts in sorted(by_fault.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        ax_acc.plot(xs, [p[1] for p in pts], "o-", label=f"{fault_type} P")
        ax_acc.plot(xs, [p[2] for p in pts], "s--", label=f"{fault_type} R")
        ax_ttd.plot(xs, [p[3] for p in pts], "o-", label=fault_type)
    ax_acc.set_xlabel("Workers")
    ax_acc.set_ylabel("Precision / Recall")
    ax_acc.set_ylim(-0.05, 1.05)
    ax_acc.grid(True, alpha=0.3)
    ax_acc.legend(fontsize=7)
    add_inexpressible_note(ax_acc, rows, "round_failure")
    ax_ttd.set_xlabel("Workers")
    ax_ttd.set_ylabel("Time to detection (ms)")
    ax_ttd.grid(True, alpha=0.3)
    ax_ttd.legend(fontsize=7)
    fig.suptitle("Online round-failure detection")
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_interference(rows, outpath: str):
    """Q5 external windowed-correlation precision/recall and latency."""
    selected = (
        plot_online_rows(rows, "interference", "sonata_external_windowed")
        + plot_online_rows(rows, "interference", "inc_aware")
    )
    if not selected:
        print("  [skip] no interference rows")
        return

    fig, (ax_acc, ax_ttd) = plt.subplots(1, 2, figsize=(7.0, 3.0))
    by_system = defaultdict(list)
    for row in selected:
        by_system[row[4].get("system", "unknown")].append(row)
    for system, pts in sorted(by_system.items()):
        pts.sort(key=lambda item: item[0])
        xs = [p[0] for p in pts]
        precision = [p[1] for p in pts]
        recall = [p[2] for p in pts]
        ttd = [p[3] for p in pts]
        ax_acc.plot(xs, precision, "o-", label=f"{system} P")
        ax_acc.plot(xs, recall, "s--", label=f"{system} R")
        ax_ttd.plot(xs, ttd, "o-", label=system)
    ax_acc.set_xlabel("Workers")
    ax_acc.set_ylabel("Precision / Recall")
    ax_acc.set_ylim(-0.05, 1.05)
    ax_acc.grid(True, alpha=0.3)
    ax_acc.legend(fontsize=7)
    add_inexpressible_note(ax_acc, rows, "interference")
    ax_ttd.set_xlabel("Workers")
    ax_ttd.set_ylabel("Time to detection (ms)")
    ax_ttd.grid(True, alpha=0.3)
    ax_ttd.legend(fontsize=7)
    fig.suptitle("Windowed cross-job interference detection")
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_univmon_key_sensitivity(rows, outpath: str):
    selected = []
    order = [
        "worker",
        "job_worker",
        "job_round_worker",
        "job_round_worker_chunk",
    ]
    order_index = {name: i for i, name in enumerate(order)}
    for r in rows:
        if r.get("workload") != "univmon_key_sensitivity":
            continue
        key_mode = r.get("key_mode")
        workers = fnum(r.get("n_workers"))
        precision = fnum(r.get("precision_mean"))
        recall = fnum(r.get("recall_mean"))
        keys = fnum(r.get("keys_tracked_mean"))
        if key_mode not in order_index or None in (workers, precision, recall, keys):
            continue
        selected.append((int(workers), order_index[key_mode], key_mode, precision, recall, keys))

    if not selected:
        print("  [skip] no univmon_key_sensitivity rows")
        return

    fig, (ax_acc, ax_keys) = plt.subplots(1, 2, figsize=(7.5, 3.0))
    by_workers = defaultdict(list)
    for row in selected:
        by_workers[row[0]].append(row)
    for workers, pts in sorted(by_workers.items()):
        pts.sort(key=lambda x: x[1])
        xs = [p[1] for p in pts]
        ax_acc.plot(xs, [p[3] for p in pts], "o-", label=f"W={workers} P")
        ax_acc.plot(xs, [p[4] for p in pts], "s--", label=f"W={workers} R")
        ax_keys.plot(xs, [p[5] for p in pts], "o-", label=f"W={workers}")
    labels = ["worker", "job,\nworker", "job,round,\nworker", "job,round,\nworker,chunk"]
    ax_acc.set_xticks(range(len(order)), labels)
    ax_acc.set_ylim(-0.05, 1.05)
    ax_acc.set_ylabel("Precision / Recall")
    ax_acc.grid(True, alpha=0.3)
    ax_acc.legend(fontsize=7, ncol=2)
    ax_keys.set_xticks(range(len(order)), labels)
    ax_keys.set_yscale("log")
    ax_keys.set_ylabel("Logical keys tracked")
    ax_keys.grid(True, alpha=0.3)
    ax_keys.legend(fontsize=7)
    fig.suptitle("UnivMon Q2 sensitivity to flow-key choice")
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_q4_timeout_sweep(rows, outpath: str):
    """Q4 detection lag vs inactivity timeout, with false-positive tradeoff."""
    fault_rows = defaultdict(list)
    healthy_rows = defaultdict(list)
    for r in rows:
        if r.get("workload") != "round_failure":
            continue
        system = r.get("system")
        if system not in {"sonata_windowed", "inc_aware"}:
            continue
        fault = r.get("fault_type")
        timeout = fnum(r.get("timeout_ms"))
        workers = fnum(r.get("n_workers"))
        if None in (timeout, workers):
            continue
        if fault in {"missing_chunks", "failed_round"}:
            ttd = fnum(r.get("time_to_detection_ms_mean"))
            if ttd is None:
                continue
            fault_rows[(system, fault, int(workers))].append((timeout, ttd))
        elif fault == "healthy_bursty":
            fpr = fnum(r.get("false_positive_rate_mean"))
            if fpr is None:
                continue
            healthy_rows[(system, int(workers))].append((timeout, fpr))

    if not fault_rows and not healthy_rows:
        print("  [skip] no round_failure timeout-sweep rows")
        return

    fig, (ax_ttd, ax_fpr) = plt.subplots(1, 2, figsize=(7.5, 3.0))
    for (system, fault, workers), pts in sorted(fault_rows.items()):
        if workers not in (8, 16):  # keep legend compact; trends are W-invariant
            continue
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        label = f"{system} {fault} W={workers}"
        ax_ttd.plot(xs, ys, "o-", label=label)
    ax_ttd.set_xscale("log")
    ax_ttd.set_xlabel("Inactivity timeout (ms)")
    ax_ttd.set_ylabel("Detection lag (ms)")
    ax_ttd.set_title("Q4 detection lag vs timeout")
    ax_ttd.grid(True, alpha=0.3)
    ax_ttd.legend(fontsize=7)

    for (system, workers), pts in sorted(healthy_rows.items()):
        if workers not in (8, 16):
            continue
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax_fpr.plot(xs, ys, "s-", label=f"{system} W={workers}")
    ax_fpr.set_xscale("log")
    ax_fpr.set_xlabel("Inactivity timeout (ms)")
    ax_fpr.set_ylabel("False-positive rate (healthy-bursty)")
    ax_fpr.set_ylim(-0.05, 1.05)
    ax_fpr.set_title("Q4 false-positive rate vs timeout")
    ax_fpr.grid(True, alpha=0.3)
    ax_fpr.legend(fontsize=7)

    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_univmon_budget(rows, outpath: str):
    """UnivMon Q2 recall vs sketch width per worker count.

    Overlays Sonata's exact recall as a horizontal reference; the gap between
    the UnivMon curves and that reference is the sketch budget UnivMon would
    have to spend to be competitive.
    """
    univmon_pts = defaultdict(list)
    sonata_ref = {}
    for r in rows:
        if r.get("workload") != "univmon_budget":
            continue
        workers = fnum(r.get("n_workers"))
        recall = fnum(r.get("recall_mean"))
        if None in (workers, recall):
            continue
        system = r.get("system")
        if system == "univmon":
            width = fnum(r.get("sketch_width"))
            if width is None:
                continue
            univmon_pts[int(workers)].append((int(width), recall))
        elif system == "sonata":
            sonata_ref[int(workers)] = recall

    if not univmon_pts:
        print("  [skip] no univmon_budget rows")
        return

    fig, ax = plt.subplots()
    for workers, pts in sorted(univmon_pts.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax.plot(xs, ys, "o-", label=f"univmon W={workers}")
        if workers in sonata_ref:
            ax.axhline(
                sonata_ref[workers],
                linestyle=":",
                alpha=0.6,
                label=f"sonata W={workers} (ref)",
            )
    ax.set_xscale("log")
    ax.set_xlabel("Sketch width (per row)")
    ax.set_ylabel("Recall")
    ax.set_ylim(-0.05, 1.05)
    ax.set_title("UnivMon Q2 recall vs sketch budget")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=7, ncol=2)
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def plot_negative_controls(rows, outpath: str):
    selected = []
    for r in rows:
        if r.get("workload") != "negative_control":
            continue
        fpr = fnum(r.get("false_positive_rate_mean"))
        workers = fnum(r.get("n_workers"))
        if None in (fpr, workers):
            continue
        label = f"{r.get('query')} {r.get('control_type')}"
        selected.append((label, int(workers), fpr))

    if not selected:
        print("  [skip] no negative_control rows")
        return

    labels = sorted({x[0] for x in selected})
    workers_values = sorted({x[1] for x in selected})
    x_positions = range(len(labels))
    width = 0.8 / max(1, len(workers_values))

    fig, ax = plt.subplots(figsize=(7.5, 3.2))
    for idx, workers in enumerate(workers_values):
        ys = []
        for label in labels:
            value = next((x[2] for x in selected if x[0] == label and x[1] == workers), 0.0)
            ys.append(value)
        xs = [x + idx * width for x in x_positions]
        ax.bar(xs, ys, width=width, label=f"W={workers}")
    ax.set_xticks(
        [x + width * (len(workers_values) - 1) / 2 for x in x_positions],
        labels,
        rotation=25,
        ha="right",
    )
    ax.set_ylabel("False-positive rate")
    ax.set_ylim(0.0, 1.0)
    ax.set_title("Negative controls")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(ncol=3, fontsize=7)
    fig.tight_layout()
    fig.savefig(outpath)
    plt.close(fig)
    print(f"  wrote {outpath}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="inp", default="results/results.csv")
    parser.add_argument("--outdir", default="results/figures")
    args = parser.parse_args()

    if args.inp == "results/results.csv" and os.path.exists("results/summary.csv"):
        args.inp = "results/summary.csv"

    if not os.path.exists(args.inp):
        print(f"ERROR: {args.inp} not found. Run evaluate.py first.")
        return 1

    os.makedirs(args.outdir, exist_ok=True)
    plt.rcParams.update(PLOT_STYLE)
    rows = load_rows(args.inp)

    print(f"Generating figures from {args.inp}...")
    plot_straggler(rows, os.path.join(args.outdir, "straggler_accuracy.pdf"))
    plot_packet_loss(rows, os.path.join(args.outdir, "packet_loss_accuracy.pdf"))
    plot_multitenant(rows, os.path.join(args.outdir, "multitenant_state.pdf"))
    plot_round_failure(rows, os.path.join(args.outdir, "round_failure_online.pdf"))
    plot_interference(rows, os.path.join(args.outdir, "interference_online.pdf"))
    plot_univmon_key_sensitivity(
        rows, os.path.join(args.outdir, "univmon_key_sensitivity.pdf")
    )
    plot_univmon_budget(rows, os.path.join(args.outdir, "univmon_budget.pdf"))
    plot_q4_timeout_sweep(rows, os.path.join(args.outdir, "q4_timeout_sweep.pdf"))
    plot_negative_controls(rows, os.path.join(args.outdir, "negative_controls.pdf"))
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
