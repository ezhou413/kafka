import argparse
import sys
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional


Timestamp = datetime
Seconds = float
Offset = int


def parse_timestamp(ts_str: str) -> Timestamp:
    return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S,%f")

def unzip(points: List[Tuple[Seconds, Offset]]) -> Tuple[List[Seconds], List[Offset]]:
    if not points:
        return [], []
    xs, ys = zip(*points)
    return list(xs), list(ys)

def compute_intervals(xs: List[Seconds]) -> Tuple[List[Seconds], List[Seconds]]:
    if len(xs) < 2:
        return [], []
    ix: List[Seconds] = []
    iy: List[Seconds] = []
    prev = xs[0]
    for t in xs[1:]:
        ix.append(t)
        iy.append(t - prev)
        prev = t
    return ix, iy


TIMESTAMP_AT_START_RE = re.compile(r"^\[(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\]")
# Timestamp anywhere in the line (e.g., after a log level prefix like `[ERROR] `)
TIMESTAMP_ANYWHERE_RE = re.compile(r"(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")

# Example:
# ... Updating last stable offset for partition my-topic-0 to 14928793 (...)
LSO_UPDATE_RE = re.compile(
    r"Updating last stable offset for partition\s+(?P<tp>[\w\.-]+-\d+)\s+to\s+(?P<lso>\d+)\b"
)

# Example:
# ... Updating fetch position from FetchPosition{offset=50155,...}
#   to FetchPosition{offset=50655,...} for partition my-topic-0 ...
FETCH_UPDATE_RE = re.compile(
    r"Updating fetch position from\s+FetchPosition\{.*?offset=(?P<from>\d+).*?}\s+"
    r"to\s+FetchPosition\{.*?offset=(?P<to>\d+).*?}\s+for partition\s+(?P<tp>[\w\.-]+-\d+)\b"
)


def find_first_timestamp(file_path: str) -> Optional[Timestamp]:
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = TIMESTAMP_ANYWHERE_RE.search(line)
                if m:
                    return parse_timestamp(m.group("ts"))
    except FileNotFoundError:
        return None
    return None


def get_start_timestamp(file_path: str) -> Optional[Timestamp]:
    # Prefer the timestamp on the very first line (typical log header), if present.
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline()
            if first_line:
                m = TIMESTAMP_ANYWHERE_RE.search(first_line)
                if m:
                    return parse_timestamp(m.group("ts"))
    except FileNotFoundError:
        return None
    # Fallback: first timestamp found in the file scanning top-down.
    return find_first_timestamp(file_path)


def parse_log(
    file_path: str,
    partition_filter: Optional[str],
) -> Dict[str, Dict[str, List[Tuple[Seconds, Offset]]]]:
    base_ts = get_start_timestamp(file_path)
    if base_ts is None:
        raise RuntimeError(f"Could not find any timestamp in log file: {file_path}")

    partitions: Dict[str, Dict[str, List[Tuple[Seconds, Offset]]]] = {}

    def ensure(tp: str) -> Dict[str, List[Tuple[Seconds, Offset]]]:
        if tp not in partitions:
            partitions[tp] = {"lso": [], "fetch": []}
        return partitions[tp]

    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # Timestamp anywhere in the line
            ts_match = TIMESTAMP_ANYWHERE_RE.search(line)
            if not ts_match:
                continue
            ts = parse_timestamp(ts_match.group("ts"))

            m_lso = LSO_UPDATE_RE.search(line)
            if m_lso:
                tp = m_lso.group("tp")
                if partition_filter is None or tp == partition_filter:
                    seconds = (ts - base_ts).total_seconds()
                    lso_val = int(m_lso.group("lso"))
                    ensure(tp)["lso"].append((seconds, lso_val))
                continue

            m_fetch = FETCH_UPDATE_RE.search(line)
            if m_fetch:
                tp = m_fetch.group("tp")
                if partition_filter is None or tp == partition_filter:
                    seconds = (ts - base_ts).total_seconds()
                    to_offset = int(m_fetch.group("to"))
                    ensure(tp)["fetch"].append((seconds, to_offset))

    return partitions


#


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Plot Last Stable Offset and Fetch Position over time for classic vs consumer logs."
        )
    )
    parser.add_argument("classic_log", help="Path to classic-protocol consumer log file")
    parser.add_argument("consumer_log", help="Path to consumer-protocol consumer log file")
    parser.add_argument(
        "--out",
        dest="out_file",
        help="Optional path to save the plot as an image (e.g., plot.png)",
    )
    parser.add_argument(
        "--no-show",
        dest="no_show",
        action="store_true",
        help="Do not display the interactive window (useful when saving to file)",
    )
    parser.add_argument(
        "--title",
        dest="title",
        default="Kafka Consumer Offsets Over Time",
        help="Custom plot title",
    )

    args = parser.parse_args()

    try:
        import matplotlib.pyplot as plt
    except Exception as e:
        print(
            "matplotlib is required for plotting. Install it with: pip install matplotlib",
            file=sys.stderr,
        )
        raise
    # Parse all partitions (no partition filtering)
    classic_parts = parse_log(args.classic_log, None)
    consumer_parts = parse_log(args.consumer_log, None)

    # Print start times of both logs for reference
    classic_start = get_start_timestamp(args.classic_log)
    consumer_start = get_start_timestamp(args.consumer_log)

    def _format_ts(ts: Optional[Timestamp]) -> str:
        if ts is None:
            return "unknown"
        s = ts.strftime("%Y-%m-%d %H:%M:%S,%f")
        return s[:-3]  # to milliseconds

    print(f"Classic log start time: {_format_ts(classic_start)}")
    print(f"Consumer log start time: {_format_ts(consumer_start)}")

    # Build series per partition for each metric
    def build_series(parts: Dict[str, Dict[str, List[Tuple[Seconds, Offset]]]], key: str) -> List[Tuple[List[Seconds], List[Offset]]]:
        series: List[Tuple[List[Seconds], List[Offset]]] = []
        for tp in sorted(parts.keys()):
            pts = parts[tp].get(key, [])
            if pts:
                xs, ys = unzip(pts)
                series.append((xs, ys))
        return series

    classic_lso_series = build_series(classic_parts, "lso")
    classic_fetch_series = build_series(classic_parts, "fetch")
    consumer_lso_series = build_series(consumer_parts, "lso")
    consumer_fetch_series = build_series(consumer_parts, "fetch")

    if not (classic_lso_series or classic_fetch_series or consumer_lso_series or consumer_fetch_series):
        raise RuntimeError("No matching data found in either log for any partition.")

    # Pre-compute intervals and max x across all series
    def interval_series(series: List[Tuple[List[Seconds], List[Offset]]]) -> List[Tuple[List[Seconds], List[Seconds]]]:
        out: List[Tuple[List[Seconds], List[Seconds]]] = []
        for xs, _ in series:
            ix, iy = compute_intervals(xs)
            if ix:
                out.append((ix, iy))
        return out

    classic_lso_intervals = interval_series(classic_lso_series)
    consumer_lso_intervals = interval_series(consumer_lso_series)
    classic_fetch_intervals = interval_series(classic_fetch_series)
    consumer_fetch_intervals = interval_series(consumer_fetch_series)

    max_x = 0.0
    for series in (classic_lso_series, classic_fetch_series, consumer_lso_series, consumer_fetch_series):
        for xs, _ in series:
            if xs:
                max_x = max(max_x, max(xs))

    classic_color = "tab:blue"
    consumer_color = "tab:orange"

    fig, axes = plt.subplots(5, 1, figsize=(17, 13), sharex=True)
    ax1, ax2, ax3, ax4, ax5 = axes
    fig.suptitle(args.title)

    # LSO value subplot (top)
    classic_lso_label_added = False
    for xs, ys in classic_lso_series:
        ax1.plot(xs, ys, label=("classic LSO" if not classic_lso_label_added else None), color=classic_color, linewidth=1.5, alpha=0.9)
        classic_lso_label_added = True
    consumer_lso_label_added = False
    for xs, ys in consumer_lso_series:
        ax1.plot(xs, ys, label=("consumer LSO" if not consumer_lso_label_added else None), color=consumer_color, linewidth=1.5, alpha=0.9)
        consumer_lso_label_added = True
    ax1.set_ylabel("Last Stable Offset")
    ax1.grid(True, linestyle=":", alpha=0.6)
    ax1.legend(loc="best")

    # LSO update interval subplot (second)
    classic_lso_dt_label_added = False
    for ix, iy in classic_lso_intervals:
        ax2.plot(ix, iy, label=("classic LSO Δt" if not classic_lso_dt_label_added else None), color=classic_color, linewidth=1.2, alpha=0.9)
        classic_lso_dt_label_added = True
    consumer_lso_dt_label_added = False
    for ix, iy in consumer_lso_intervals:
        ax2.plot(ix, iy, label=("consumer LSO Δt" if not consumer_lso_dt_label_added else None), color=consumer_color, linewidth=1.2, alpha=0.9)
        consumer_lso_dt_label_added = True
    ax2.set_ylabel("LSO Update Interval (s)")
    ax2.grid(True, linestyle=":", alpha=0.6)
    ax2.legend(loc="best")

    # Fetch position value subplot (third)
    classic_fetch_label_added = False
    for xs, ys in classic_fetch_series:
        ax3.plot(xs, ys, label=("classic fetch position" if not classic_fetch_label_added else None), color=classic_color, linewidth=1.5, alpha=0.9)
        classic_fetch_label_added = True
    consumer_fetch_label_added = False
    for xs, ys in consumer_fetch_series:
        ax3.plot(xs, ys, label=("consumer fetch position" if not consumer_fetch_label_added else None), color=consumer_color, linewidth=1.5, alpha=0.9)
        consumer_fetch_label_added = True
    ax3.set_ylabel("Fetch Position")
    ax3.grid(True, linestyle=":", alpha=0.6)
    ax3.legend(loc="best")

    # Fetch update interval subplot (fourth - classic)
    classic_fetch_dt_label_added = False
    for ix, iy in classic_fetch_intervals:
        ax4.plot(ix, iy, label=("classic fetch Δt" if not classic_fetch_dt_label_added else None), color=classic_color, linewidth=1.2, alpha=0.9)
        classic_fetch_dt_label_added = True
    ax4.set_ylabel("Fetch Update Interval (s)")
    ax4.grid(True, linestyle=":", alpha=0.6)
    ax4.legend(loc="best")

    # Fetch update interval subplot (bottom - consumer)
    consumer_fetch_dt_label_added = False
    for ix, iy in consumer_fetch_intervals:
        ax5.plot(ix, iy, label=("consumer fetch Δt" if not consumer_fetch_dt_label_added else None), color=consumer_color, linewidth=1.2, alpha=0.9)
        consumer_fetch_dt_label_added = True
    ax5.set_xlabel("Seconds from start of log")
    ax5.set_ylabel("Fetch Update Interval (s)")
    ax5.grid(True, linestyle=":", alpha=0.6)
    ax5.legend(loc="best")

    if max_x > 0:
        ax5.set_xlim(0, max_x * 1.02)

    fig.tight_layout(rect=[0, 0.02, 1, 0.95])

    if args.out_file:
        fig.savefig(args.out_file, dpi=150)
        print(f"Saved plot to {args.out_file}")
    if not args.no_show:
        plt.show()


if __name__ == "__main__":
    main()
