#!/usr/bin/env python3

import argparse
import datetime as _dt
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


# ----------------------------
# Parsing utilities
# ----------------------------

_LOG_PREFIX_TS_RE = re.compile(
    r"^\[(?P<ymd>\d{4}-\d{2}-\d{2})\s+(?P<hms>\d{2}:\d{2}:\d{2}),(?P<ms>\d{3})\]"
)

_START_TIME_NUMERIC_RE = re.compile(
    r"(?i)start(?:ing)?\s*time\w*\s*[:=]\s*(?P<ms>\d{10,})"
)
_START_TIME_ISO_RE = re.compile(
    r"(?i)start(?:ing)?\s*time\w*\s*[:=]\s*(?P<iso>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3})"
)

_LAG_RE = re.compile(r"(?i)current\s+partition\s+lag\s+is\s+(?P<lag>\d+)")
_ISO_LSO_HWM_RE = re.compile(
    r"(?i)isolation\s+level:\s*\w+\s*,\s*LSO:\s*(?P<lso>\d+)\s*,\s*HWM:\s*(?P<hwm>\d+)\s*,\s*offset:\s*(?P<offset>\d+)"
)
_TRY_HWM_RE = re.compile(
    r"(?i)tryUpdatingHWM\s*-\s*time:\s*(?P<time>\d{10,})\s*,?\s*HWM:\s*(?P<hwm>\d+)"
)
_TRY_LSO_RE = re.compile(
    r"(?i)tryUpdatingLSO\s*-\s*time:\s*(?P<time>\d{10,})\s*,?\s*LSO:\s*(?P<lso>\d+)"
)


def _to_epoch_ms_from_prefix(line: str) -> Optional[int]:
    m = _LOG_PREFIX_TS_RE.match(line)
    if not m:
        return None
    ymd = m.group("ymd")
    hms = m.group("hms")
    ms = int(m.group("ms"))
    dt = _dt.datetime.strptime(f"{ymd} {hms}", "%Y-%m-%d %H:%M:%S")
    # interpret as local time
    epoch_ms = int(dt.timestamp() * 1000) + ms
    return epoch_ms


def _to_epoch_ms_from_iso(s: str) -> Optional[int]:
    try:
        dt = _dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f")
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _find_start_time_ms(lines: Iterable[str]) -> Optional[int]:
    for line in lines:
        m1 = _START_TIME_NUMERIC_RE.search(line)
        if m1:
            ms = int(m1.group("ms"))
            if len(str(ms)) == 10:
                ms *= 1000
            return ms
        m2 = _START_TIME_ISO_RE.search(line)
        if m2:
            iso = m2.group("iso")
            ms = _to_epoch_ms_from_iso(iso)
            if ms is not None:
                return ms
    return None


def _find_time_field_ms(line: str) -> Optional[int]:
    m = re.search(r"(?i)\btime:\s*(\d{10,})", line)
    if not m:
        return None
    value = int(m.group(1))
    if len(str(value)) == 10:
        value *= 1000
    return value


def parse_log(path: Path) -> Dict[str, List[Tuple[float, int]]]:
    # First pass: detect explicit start time (if present)
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            start_time_ms = _find_start_time_ms(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Log file not found: {path}")

    events: Dict[str, List[Tuple[int, int]]] = {"lag": [], "hwm": [], "offset": []}
    earliest_event_time_ms: Optional[int] = None

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            # Determine line time
            time_ms = _find_time_field_ms(line)
            if time_ms is None:
                time_ms = _to_epoch_ms_from_prefix(line)

            # Track earliest time among relevant lines
            if time_ms is not None and (earliest_event_time_ms is None or time_ms < earliest_event_time_ms):
                earliest_event_time_ms = time_ms

            # Parse lag lines
            m_lag = _LAG_RE.search(line)
            if m_lag and time_ms is not None:
                lag_val = int(m_lag.group("lag"))
                events["lag"].append((time_ms, lag_val))
                continue

            # Parse isolation-level line that has LSO/HWM/offset
            m_iso = _ISO_LSO_HWM_RE.search(line)
            if m_iso and time_ms is not None:
                hwm_val = int(m_iso.group("hwm"))
                offset_val = int(m_iso.group("offset"))
                events["hwm"].append((time_ms, hwm_val))
                events["offset"].append((time_ms, offset_val))
                continue

            # Parse tryUpdatingHWM
            m_try_hwm = _TRY_HWM_RE.search(line)
            if m_try_hwm:
                time_ms_hwm = int(m_try_hwm.group("time"))
                if len(str(time_ms_hwm)) == 10:
                    time_ms_hwm *= 1000
                hwm_val = int(m_try_hwm.group("hwm"))
                events["hwm"].append((time_ms_hwm, hwm_val))
                if earliest_event_time_ms is None or time_ms_hwm < earliest_event_time_ms:
                    earliest_event_time_ms = time_ms_hwm
                continue

            # Ignore tryUpdatingLSO for plotting; still accounted in earliest time via time field above

    # Resolve start time
    if start_time_ms is None:
        start_time_ms = earliest_event_time_ms if earliest_event_time_ms is not None else 0

    # Convert to relative seconds and sort by time
    series: Dict[str, List[Tuple[float, int]]] = {"lag": [], "hwm": [], "offset": []}
    for key, tuples in events.items():
        tuples.sort(key=lambda t: t[0])
        for t_ms, val in tuples:
            rel_s = max(0.0, (t_ms - start_time_ms) / 1000.0)
            series[key].append((rel_s, val))
    # Build cumulative count for lag lines
    lag_times = [t for t, _ in series["lag"]]
    series["lag_count"] = [(t, idx + 1) for idx, t in enumerate(lag_times)]
    return series


def _default_paths(script_dir: Path) -> Tuple[Path, Path, Path]:
    classic = script_dir / "classic.txt"
    consumer = script_dir / "consumer.txt"
    out = script_dir / "lag_hwm_lso_comparison.png"
    return classic, consumer, out


def _plot(classic: Dict[str, List[Tuple[float, int]]], consumer: Dict[str, List[Tuple[float, int]]], out_path: Path) -> None:
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        print("matplotlib is required to plot. Install with: pip install matplotlib", file=sys.stderr)
        raise

    fig, axes = plt.subplots(5, 1, figsize=(12, 15), sharex=True)

    def _draw(ax, key: str, ylabel: str):
        x_c, y_c = zip(*classic[key]) if classic[key] else ([], [])
        x_n, y_n = zip(*consumer[key]) if consumer[key] else ([], [])
        ax.plot(x_c, y_c, label="classic", color="tab:blue", linewidth=1.6)
        ax.plot(x_n, y_n, label="consumer", color="tab:orange", linewidth=1.6)
        ax.set_ylabel(ylabel)
        ax.grid(True, linestyle=":", linewidth=0.8, alpha=0.7)
        ax.legend(loc="best")

    _draw(axes[0], "lag", "Lag (messages)")
    _draw(axes[1], "hwm", "HWM (offset)")
    _draw(axes[2], "offset", "Offset")

    # Count subplot (cumulative occurrences of lag lines)
    def _draw_count(ax):
        x_c, y_c = zip(*classic["lag_count"]) if classic.get("lag_count") else ([], [])
        x_n, y_n = zip(*consumer["lag_count"]) if consumer.get("lag_count") else ([], [])
        if x_c:
            ax.step(x_c, y_c, where="post", label="classic", color="tab:blue", linewidth=1.6)
        if x_n:
            ax.step(x_n, y_n, where="post", label="consumer", color="tab:orange", linewidth=1.6)
        ax.set_ylabel("Lag line count")
        ax.grid(True, linestyle=":", linewidth=0.8, alpha=0.7)
        ax.legend(loc="best")

    _draw_count(axes[3])

    # Inter-update time gaps for HWM and Offset
    def _gaps(points: List[Tuple[float, int]]):
        if not points or len(points) < 2:
            return ([], [])
        times: List[float] = []
        deltas: List[float] = []
        prev_t = points[0][0]
        for i in range(1, len(points)):
            t = points[i][0]
            times.append(t)
            deltas.append(max(0.0, t - prev_t))
            prev_t = t
        return times, deltas

    def _draw_update_gaps(ax):
        # classic series
        x_ch, y_ch = _gaps(classic.get("hwm", []))
        x_co, y_co = _gaps(classic.get("offset", []))
        # consumer series
        x_nh, y_nh = _gaps(consumer.get("hwm", []))
        x_no, y_no = _gaps(consumer.get("offset", []))

        if x_ch:
            ax.plot(x_ch, y_ch, label="classic HWM Δt", color="tab:blue", linewidth=1.6, linestyle="-")
        if x_co:
            ax.plot(x_co, y_co, label="classic offset Δt", color="tab:blue", linewidth=1.6, linestyle="--")
        if x_nh:
            ax.plot(x_nh, y_nh, label="consumer HWM Δt", color="tab:orange", linewidth=1.6, linestyle="-")
        if x_no:
            ax.plot(x_no, y_no, label="consumer offset Δt", color="tab:orange", linewidth=1.6, linestyle="--")

        ax.set_ylabel("Δt (s)")
        ax.grid(True, linestyle=":", linewidth=0.8, alpha=0.7)
        ax.legend(loc="best")

    _draw_update_gaps(axes[4])

    axes[4].set_xlabel("Time since start (s)")
    fig.suptitle("Classic vs Consumer: Lag, HWM, Offset, Lag-Line Count, Update Δt")
    fig.tight_layout(rect=[0, 0.03, 1, 0.97])
    fig.savefig(out_path, dpi=150)
    print(f"Saved plot → {out_path}")


def main(argv: Optional[List[str]] = None) -> int:
    script_dir = Path(__file__).resolve().parent
    default_classic, default_consumer, default_out = _default_paths(script_dir)

    parser = argparse.ArgumentParser(description="Parse Kafka consumer logs and plot Lag/HWM/LSO for classic vs consumer protocols.")
    parser.add_argument("--classic", type=Path, default=default_classic, help="Path to classic protocol log file")
    parser.add_argument("--consumer", type=Path, default=default_consumer, help="Path to consumer protocol log file")
    parser.add_argument("--out", type=Path, default=default_out, help="Output PNG path for the plots")
    args = parser.parse_args(argv)

    classic_series = parse_log(args.classic)
    consumer_series = parse_log(args.consumer)

    _plot(classic_series, consumer_series, args.out)

    # Print update counts to terminal
    classic_lag_updates = len(classic_series.get("lag", []))
    classic_hwm_updates = len(classic_series.get("hwm", []))
    classic_offset_updates = len(classic_series.get("offset", []))
    consumer_lag_updates = len(consumer_series.get("lag", []))
    consumer_hwm_updates = len(consumer_series.get("hwm", []))
    consumer_offset_updates = len(consumer_series.get("offset", []))

    print(
        f"Classic updates - lag: {classic_lag_updates}, HWM: {classic_hwm_updates}, offset: {classic_offset_updates}"
    )
    print(
        f"Consumer updates - lag: {consumer_lag_updates}, HWM: {consumer_hwm_updates}, offset: {consumer_offset_updates}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
