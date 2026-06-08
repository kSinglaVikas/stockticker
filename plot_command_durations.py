import argparse
import os
import re
import statistics
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt


SUCCESS_RE = re.compile(r"\[COMMAND_SUCCEEDED\]\s+cmd=(\w+)\s+duration_µs=(\d+)")
TARGET_COMMANDS = ("find", "aggregate")


def percentile(sorted_values: List[int], p: float) -> float:
    if not sorted_values:
        raise ValueError("Cannot compute percentile of empty list")

    idx = (len(sorted_values) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    if lo == hi:
        return float(sorted_values[lo])

    frac = idx - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def parse_log(log_path: str) -> Dict[str, List[int]]:
    durations: Dict[str, List[int]] = {"find": [], "aggregate": []}

    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = SUCCESS_RE.search(line)
            if not m:
                continue

            cmd, duration_us = m.group(1), m.group(2)
            if cmd not in durations:
                continue

            durations[cmd].append(int(duration_us))

    return durations


def summarize(values: List[int]) -> Dict[str, float]:
    ordered = sorted(values)
    return {
        "count": float(len(ordered)),
        "min": float(ordered[0]),
        "avg": float(statistics.mean(ordered)),
        "p50": percentile(ordered, 0.50),
        "p90": percentile(ordered, 0.90),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "p99.9": percentile(ordered, 0.999),
        "max": float(ordered[-1]),
    }


def write_summary(summary_path: str, by_cmd: Dict[str, List[int]]) -> None:
    lines: List[str] = []

    for cmd in TARGET_COMMANDS:
        values = by_cmd.get(cmd, [])
        lines.append(f"[{cmd}]")
        if not values:
            lines.append("count=0")
            lines.append("")
            continue

        stats = summarize(values)
        lines.append(f"count={int(stats['count'])}")
        lines.append(f"min={int(stats['min'])}")
        lines.append(f"avg={stats['avg']:.2f}")
        lines.append(f"p50={stats['p50']:.2f}")
        lines.append(f"p90={stats['p90']:.2f}")
        lines.append(f"p95={stats['p95']:.2f}")
        lines.append(f"p99={stats['p99']:.2f}")
        lines.append(f"p99.9={stats['p99.9']:.2f}")
        lines.append(f"max={int(stats['max'])}")
        lines.append("")

    with open(summary_path, "w", encoding="utf-8") as out:
        out.write("\n".join(lines).rstrip() + "\n")


def _sample_for_scatter(values: List[int], max_points: int) -> Tuple[List[int], List[int]]:
    if len(values) <= max_points:
        x = list(range(1, len(values) + 1))
        return x, values

    step = max(1, len(values) // max_points)
    sampled = values[::step][:max_points]
    x = [1 + i * step for i in range(len(sampled))]
    return x, sampled


def _minmax_normalize(values: List[int]) -> List[float]:
    if not values:
        return []
    v_min = min(values)
    v_max = max(values)
    if v_max == v_min:
        return [0.5 for _ in values]
    scale = float(v_max - v_min)
    return [(v - v_min) / scale for v in values]


def _ecdf(values: List[float]) -> Tuple[List[float], List[float]]:
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return [], []
    y = [(i + 1) / n for i in range(n)]
    return ordered, y


def plot_simple_hist(output_png: str, by_cmd: Dict[str, List[int]], bins: int) -> None:
    find_values = by_cmd.get("find", [])
    agg_values = by_cmd.get("aggregate", [])

    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    ax_find = axes[0]
    ax_agg = axes[1]

    if find_values:
        ax_find.hist(find_values, bins=bins, color="#1f77b4", alpha=0.8)
        ax_find.set_title("find")
    else:
        ax_find.set_title("find (no data)")

    if agg_values:
        ax_agg.hist(agg_values, bins=bins, color="#ff7f0e", alpha=0.8)
        ax_agg.set_title("aggregate")
    else:
        ax_agg.set_title("aggregate (no data)")

    ax_find.set_xlabel("duration (µs)")
    ax_agg.set_xlabel("duration (µs)")
    ax_find.set_ylabel("count")

    fig.suptitle("COMMAND_SUCCEEDED latency histogram")
    fig.tight_layout()
    fig.savefig(output_png, dpi=150)
    plt.close(fig)


def plot_normalized(output_png: str, by_cmd: Dict[str, List[int]], max_scatter_points: int) -> None:
    find_values = by_cmd.get("find", [])
    agg_values = by_cmd.get("aggregate", [])
    find_norm = _minmax_normalize(find_values)
    agg_norm = _minmax_normalize(agg_values)

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    ax_seq = axes[0][0]
    ax_hist = axes[0][1]
    ax_ecdf = axes[1][0]
    ax_box = axes[1][1]

    if find_norm:
        x_find, y_find = _sample_for_scatter(find_norm, max_scatter_points)
        ax_seq.plot(x_find, y_find, linewidth=0.7, alpha=0.8, color="#1f77b4", label="find")
    if agg_norm:
        x_agg, y_agg = _sample_for_scatter(agg_norm, max_scatter_points)
        ax_seq.plot(x_agg, y_agg, linewidth=0.7, alpha=0.8, color="#ff7f0e", label="aggregate")
    ax_seq.set_title("normalized latency over sequence (min-max per command)")
    ax_seq.set_xlabel("query index")
    ax_seq.set_ylabel("normalized duration [0,1]")
    if find_norm or agg_norm:
        ax_seq.legend()

    bins = 80
    if find_norm:
        ax_hist.hist(find_norm, bins=bins, density=True, alpha=0.45, label="find", color="#1f77b4")
    if agg_norm:
        ax_hist.hist(agg_norm, bins=bins, density=True, alpha=0.45, label="aggregate", color="#ff7f0e")
    ax_hist.set_title("normalized density")
    ax_hist.set_xlabel("normalized duration [0,1]")
    ax_hist.set_ylabel("density")
    if find_norm or agg_norm:
        ax_hist.legend()

    if find_norm:
        x_f, y_f = _ecdf(find_norm)
        ax_ecdf.plot(x_f, y_f, color="#1f77b4", linewidth=1.2, label="find")
    if agg_norm:
        x_a, y_a = _ecdf(agg_norm)
        ax_ecdf.plot(x_a, y_a, color="#ff7f0e", linewidth=1.2, label="aggregate")
    ax_ecdf.set_title("normalized ECDF")
    ax_ecdf.set_xlabel("normalized duration [0,1]")
    ax_ecdf.set_ylabel("cumulative probability")
    if find_norm or agg_norm:
        ax_ecdf.legend()

    labels: List[str] = []
    box_data: List[List[float]] = []
    if find_norm:
        labels.append("find")
        box_data.append(find_norm)
    if agg_norm:
        labels.append("aggregate")
        box_data.append(agg_norm)

    if box_data:
        ax_box.boxplot(box_data, tick_labels=labels, showfliers=False)
    ax_box.set_title("normalized boxplot")
    ax_box.set_ylabel("normalized duration [0,1]")

    fig.tight_layout()
    fig.savefig(output_png, dpi=150)
    plt.close(fig)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse benchmark log, filter COMMAND_SUCCEEDED for find/aggregate, and plot latency charts."
    )
    parser.add_argument("log_file", help="Path to benchmark log file")
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Output prefix for generated files (default: derived from input log filename)",
    )
    parser.add_argument(
        "--max-scatter-points",
        type=int,
        default=20000,
        help="Maximum points to render per command in normalized sequence plots",
    )
    parser.add_argument(
        "--bins",
        type=int,
        default=100,
        help="Histogram bin count for simple histogram mode",
    )
    parser.add_argument(
        "--plot-mode",
        choices=["simple", "normalized"],
        default="simple",
        help="Plot mode: simple (x=duration, y=count) or normalized",
    )

    args = parser.parse_args()

    if not os.path.exists(args.log_file):
        raise FileNotFoundError(f"Input file not found: {args.log_file}")

    prefix = args.output_prefix
    if not prefix:
        base = os.path.basename(args.log_file)
        stem = os.path.splitext(base)[0]
        prefix = f"{stem}_cmd_latency"

    by_cmd = parse_log(args.log_file)

    if not by_cmd["find"] and not by_cmd["aggregate"]:
        raise RuntimeError("No COMMAND_SUCCEEDED rows found for cmd=find or cmd=aggregate")

    summary_path = f"{prefix}_summary.txt"
    plot_path = f"{prefix}_plot.png"

    write_summary(summary_path, by_cmd)
    if args.plot_mode == "simple":
        plot_simple_hist(plot_path, by_cmd, bins=args.bins)
    else:
        plot_normalized(plot_path, by_cmd, max_scatter_points=args.max_scatter_points)

    print("Generated files:")
    print(f"  {summary_path}")
    print(f"  {plot_path}")
    print(f"  find_count={len(by_cmd['find'])}")
    print(f"  aggregate_count={len(by_cmd['aggregate'])}")


if __name__ == "__main__":
    main()
