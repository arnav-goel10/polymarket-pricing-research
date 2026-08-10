#!/usr/bin/env python3
"""Analyze BTC 5m Polymarket shadow strategy data.

Stdlib-only by design: this script is intended to run in the repo without
package installs. It reads the shadow ledger/state files, performs sanity checks,
computes window-level strategy statistics, scores forecast quality, and can write
a Markdown audit report.
"""

from __future__ import annotations

import argparse
import collections
import dataclasses
import json
import math
import random
import re
import statistics
import sys
from pathlib import Path
from typing import Any, Iterable


CURRENT_MODES = {"hold", "active", "conv"}
LEGACY_MODES = {"maker"}
ALL_KNOWN_MODES = CURRENT_MODES | LEGACY_MODES
REQUIRED_TRADE_KEYS = {
    "t0_millis",
    "strategy",
    "mode",
    "detail",
    "outcome_up",
    "won",
    "pnl",
}
DEFAULT_BUDGET_USD = 10.0


@dataclasses.dataclass(frozen=True)
class Trade:
    t0_millis: int
    strategy: str
    mode: str
    detail: str
    outcome_up: bool
    won: bool
    pnl: float
    legacy: bool
    entry_secs_remaining: int | None
    conv_placeholder_outcome: bool


@dataclasses.dataclass
class StrategyStats:
    mode: str
    strategy: str
    raw_trades: int
    windows: int
    raw_wins: int
    window_wins: int
    total_pnl: float
    mean_pnl: float
    median_pnl: float
    stdev_pnl: float
    min_pnl: float
    p05_pnl: float
    max_pnl: float
    max_drawdown: float
    profit_factor: float
    expectancy: float
    skew: float
    approx_roi_pct: float
    boot_ci_low: float
    boot_ci_high: float
    t_p_value: float
    sign_p_value: float
    holm_p_value: float
    trimmed_mean_pnl: float


@dataclasses.dataclass
class ForecastStats:
    name: str
    n: int
    brier: float
    log_loss: float
    directional_accuracy: float
    mean_pred: float
    up_rate: float
    brier_vs_half: float
    brier_vs_climatology: float
    logloss_vs_half: float
    logloss_vs_climatology: float
    calibration_bins: list[dict[str, float]]


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def parse_entry_secs(detail: str) -> int | None:
    match = re.search(r"@t-(\d+)s", detail)
    return int(match.group(1)) if match else None


def normalize_trades(raw: Any) -> tuple[list[Trade], list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    trades: list[Trade] = []
    if not isinstance(raw, list):
        return [], ["trades.json must be a JSON array"], warnings

    for idx, row in enumerate(raw):
        if not isinstance(row, dict):
            errors.append(f"trade[{idx}] is not an object")
            continue
        missing = REQUIRED_TRADE_KEYS - set(row)
        if missing:
            errors.append(f"trade[{idx}] missing keys: {sorted(missing)}")
            continue
        try:
            t0 = int(row["t0_millis"])
            strategy = str(row["strategy"])
            mode = str(row["mode"])
            detail = str(row["detail"])
            outcome_up = bool(row["outcome_up"])
            won = bool(row["won"])
            pnl = float(row["pnl"])
        except (TypeError, ValueError) as exc:
            errors.append(f"trade[{idx}] has malformed field: {exc}")
            continue

        if mode not in ALL_KNOWN_MODES:
            errors.append(f"trade[{idx}] has unknown mode {mode!r}")
        if not math.isfinite(pnl):
            errors.append(f"trade[{idx}] has non-finite pnl {pnl!r}")
        if t0 <= 0:
            errors.append(f"trade[{idx}] has non-positive t0_millis {t0}")
        if not strategy:
            errors.append(f"trade[{idx}] has empty strategy")

        conv_placeholder = mode == "conv" and detail.startswith("converged") and not outcome_up
        trades.append(
            Trade(
                t0_millis=t0,
                strategy=strategy,
                mode=mode,
                detail=detail,
                outcome_up=outcome_up,
                won=won,
                pnl=pnl,
                legacy=mode in LEGACY_MODES,
                entry_secs_remaining=parse_entry_secs(detail),
                conv_placeholder_outcome=conv_placeholder,
            )
        )

    exact = collections.Counter(
        (t.t0_millis, t.strategy, t.mode, t.detail, t.pnl) for t in trades
    )
    duplicate_count = sum(v - 1 for v in exact.values() if v > 1)
    if duplicate_count:
        warnings.append(f"exact duplicate-like ledger rows: {duplicate_count}")

    unknown_current = sorted({t.mode for t in trades} - ALL_KNOWN_MODES)
    if unknown_current:
        errors.append(f"unknown ledger modes found: {unknown_current}")

    return trades, errors, warnings


def group_window_pnls(trades: Iterable[Trade]) -> dict[tuple[str, str], list[tuple[int, float]]]:
    grouped: dict[tuple[str, str, int], float] = collections.defaultdict(float)
    for t in trades:
        grouped[(t.mode, t.strategy, t.t0_millis)] += t.pnl
    out: dict[tuple[str, str], list[tuple[int, float]]] = collections.defaultdict(list)
    for (mode, strategy, t0), pnl in grouped.items():
        out[(mode, strategy)].append((t0, pnl))
    for values in out.values():
        values.sort(key=lambda item: item[0])
    return dict(out)


def quantile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    pos = (len(sorted_values) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return sorted_values[lo]
    weight = pos - lo
    return sorted_values[lo] * (1 - weight) + sorted_values[hi] * weight


def max_drawdown(values: list[tuple[int, float]]) -> float:
    peak = 0.0
    equity = 0.0
    drawdown = 0.0
    for _, pnl in values:
        equity += pnl
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return drawdown


def skewness(values: list[float]) -> float:
    if len(values) < 3:
        return 0.0
    mean = statistics.fmean(values)
    pstdev = statistics.pstdev(values)
    if pstdev == 0.0:
        return 0.0
    return statistics.fmean(((x - mean) / pstdev) ** 3 for x in values)


def normal_two_sided_p(z: float) -> float:
    return max(0.0, min(1.0, math.erfc(abs(z) / math.sqrt(2.0))))


def mean_t_p_value(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 1.0
    sd = statistics.stdev(values)
    if sd == 0:
        return 0.0 if statistics.fmean(values) != 0 else 1.0
    z = statistics.fmean(values) / (sd / math.sqrt(n))
    return normal_two_sided_p(z)


def sign_test_p_value(values: list[float]) -> float:
    positives = sum(1 for v in values if v > 0)
    negatives = sum(1 for v in values if v < 0)
    n = positives + negatives
    if n == 0:
        return 1.0
    k = min(positives, negatives)
    # Exact two-sided binomial test under p=0.5.
    tail = sum(math.comb(n, i) for i in range(k + 1)) / (2**n)
    return min(1.0, 2.0 * tail)


def bootstrap_mean_ci(values: list[float], samples: int = 20_000, seed: int = 1729) -> tuple[float, float]:
    if not values:
        return (0.0, 0.0)
    if len(values) == 1:
        return (values[0], values[0])
    rng = random.Random(seed)
    n = len(values)
    means = []
    for _ in range(samples):
        means.append(sum(values[rng.randrange(n)] for _ in range(n)) / n)
    means.sort()
    return (quantile(means, 0.025), quantile(means, 0.975))


def holm_adjust(pairs: list[tuple[tuple[str, str], float]]) -> dict[tuple[str, str], float]:
    if not pairs:
        return {}
    ordered = sorted(pairs, key=lambda item: item[1])
    m = len(ordered)
    adjusted: dict[tuple[str, str], float] = {}
    running = 0.0
    for rank, (key, p_value) in enumerate(ordered, start=1):
        adj = min(1.0, (m - rank + 1) * p_value)
        running = max(running, adj)
        adjusted[key] = running
    return adjusted


def compute_strategy_stats(trades: list[Trade]) -> list[StrategyStats]:
    window_pnls = group_window_pnls(trades)
    raw_by_group: dict[tuple[str, str], list[Trade]] = collections.defaultdict(list)
    for t in trades:
        raw_by_group[(t.mode, t.strategy)].append(t)

    raw_p_values: list[tuple[tuple[str, str], float]] = []
    for key, values in window_pnls.items():
        if key[0] in CURRENT_MODES:
            raw_p_values.append((key, mean_t_p_value([pnl for _, pnl in values])))
    adjusted = holm_adjust(raw_p_values)

    stats: list[StrategyStats] = []
    for key, values in sorted(window_pnls.items()):
        mode, strategy = key
        pnls = [pnl for _, pnl in values]
        sorted_pnls = sorted(pnls)
        positives = sum(p for p in pnls if p > 0)
        negatives = -sum(p for p in pnls if p < 0)
        stdev = statistics.stdev(pnls) if len(pnls) > 1 else 0.0
        raw_rows = raw_by_group[key]
        trimmed = pnls[:]
        if len(trimmed) >= 20:
            trimmed = sorted(trimmed)[1:-1]
        ci_low, ci_high = bootstrap_mean_ci(pnls)
        stats.append(
            StrategyStats(
                mode=mode,
                strategy=strategy,
                raw_trades=len(raw_rows),
                windows=len(values),
                raw_wins=sum(1 for t in raw_rows if t.won),
                window_wins=sum(1 for pnl in pnls if pnl > 0),
                total_pnl=sum(pnls),
                mean_pnl=statistics.fmean(pnls),
                median_pnl=statistics.median(pnls),
                stdev_pnl=stdev,
                min_pnl=min(pnls),
                p05_pnl=quantile(sorted_pnls, 0.05),
                max_pnl=max(pnls),
                max_drawdown=max_drawdown(values),
                profit_factor=(positives / negatives) if negatives > 0 else math.inf,
                expectancy=statistics.fmean(pnls),
                skew=skewness(pnls),
                approx_roi_pct=(sum(pnls) / (len(values) * DEFAULT_BUDGET_USD) * 100.0),
                boot_ci_low=ci_low,
                boot_ci_high=ci_high,
                t_p_value=mean_t_p_value(pnls),
                sign_p_value=sign_test_p_value(pnls),
                holm_p_value=adjusted.get(key, math.nan),
                trimmed_mean_pnl=statistics.fmean(trimmed) if trimmed else 0.0,
            )
        )
    return stats


def log_loss(pred: float, outcome: bool) -> float:
    eps = 1e-15
    p = min(max(pred, eps), 1.0 - eps)
    return -math.log(p if outcome else 1.0 - p)


def calibration_bins(points: list[tuple[float, bool]], bins: int = 5) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    for i in range(bins):
        lo = i / bins
        hi = (i + 1) / bins
        if i == bins - 1:
            rows = [(p, y) for p, y in points if lo <= p <= hi]
        else:
            rows = [(p, y) for p, y in points if lo <= p < hi]
        if not rows:
            continue
        out.append(
            {
                "lo": lo,
                "hi": hi,
                "n": float(len(rows)),
                "avg_pred": statistics.fmean(p for p, _ in rows),
                "observed": statistics.fmean(1.0 if y else 0.0 for _, y in rows),
            }
        )
    return out


def compute_forecast_stats(run_state: dict[str, Any]) -> list[ForecastStats]:
    boards = run_state.get("tracker", {}).get("boards", {})
    out: list[ForecastStats] = []
    if not isinstance(boards, dict):
        return out
    for name, board in boards.items():
        rows = board.get("window", []) if isinstance(board, dict) else []
        points: list[tuple[float, bool]] = []
        for row in rows:
            try:
                p = float(row["p_yes_predicted"])
                y = bool(row["outcome_up"])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(p):
                points.append((min(max(p, 0.0), 1.0), y))
        if not points:
            continue
        n = len(points)
        ybar = statistics.fmean(1.0 if y else 0.0 for _, y in points)
        brier = statistics.fmean((p - (1.0 if y else 0.0)) ** 2 for p, y in points)
        brier_half = 0.25
        brier_clim = statistics.fmean((ybar - (1.0 if y else 0.0)) ** 2 for _, y in points)
        ll = statistics.fmean(log_loss(p, y) for p, y in points)
        ll_half = math.log(2.0)
        ll_clim = statistics.fmean(log_loss(ybar, y) for _, y in points)
        out.append(
            ForecastStats(
                name=str(name),
                n=n,
                brier=brier,
                log_loss=ll,
                directional_accuracy=statistics.fmean(
                    1.0 if ((p >= 0.5) == y) else 0.0 for p, y in points
                ),
                mean_pred=statistics.fmean(p for p, _ in points),
                up_rate=ybar,
                brier_vs_half=brier_half - brier,
                brier_vs_climatology=brier_clim - brier,
                logloss_vs_half=ll_half - ll,
                logloss_vs_climatology=ll_clim - ll,
                calibration_bins=calibration_bins(points),
            )
        )
    out.sort(key=lambda item: (item.brier, item.log_loss))
    return out


def data_quality(
    trades: list[Trade],
    history: list[Any],
    run_state: dict[str, Any],
    latest_state: dict[str, Any],
    normalize_warnings: list[str],
) -> list[str]:
    notes = list(normalize_warnings)
    if not trades:
        notes.append("no trades found")
        return notes

    modes = collections.Counter(t.mode for t in trades)
    if modes.get("maker", 0):
        notes.append(f"legacy maker rows present: {modes['maker']} rows; exclude from current-strategy claims")
    conv_placeholders = sum(1 for t in trades if t.conv_placeholder_outcome)
    if conv_placeholders:
        notes.append(
            f"convergence intraday close rows with placeholder outcome_up=false: {conv_placeholders}; use pnl/won, not outcome_up, for these rows"
        )

    trade_windows = len({t.t0_millis for t in trades})
    history_points = len(history) if isinstance(history, list) else 0
    if history_points and abs(history_points - trade_windows) > 5:
        notes.append(
            f"history point count ({history_points}) differs materially from trade windows ({trade_windows}); ledger is the source of truth"
        )

    scored = run_state.get("tracker", {}).get("resolved")
    if isinstance(scored, int):
        current_trade_windows = len({t.t0_millis for t in trades if t.mode in CURRENT_MODES})
        if scored != current_trade_windows:
            notes.append(
                f"forecast scoreboard resolved windows ({scored}) differs from current-mode trade windows ({current_trade_windows})"
            )

    expected_current = {"hold", "active", "conv"}
    present_current = {t.mode for t in trades if t.mode in CURRENT_MODES}
    missing = sorted(expected_current - present_current)
    if missing:
        notes.append(f"current-code modes missing from ledger: {missing}")

    by_mode_window = collections.defaultdict(int)
    for t in trades:
        if t.mode in CURRENT_MODES:
            by_mode_window[(t.mode, t.t0_millis)] += 1
    for mode in sorted(CURRENT_MODES):
        counts = [count for (m, _), count in by_mode_window.items() if m == mode]
        if counts and min(counts) < max(counts):
            notes.append(
                f"{mode} rows per window are uneven: min={min(counts)}, max={max(counts)}; strategy set changed or some windows were partial"
            )

    if isinstance(latest_state, dict):
        resolved = latest_state.get("resolved_windows")
        shadow_fills = latest_state.get("shadow_fills")
        if resolved is not None and scored is not None and resolved != scored:
            notes.append(f"latest_state resolved_windows ({resolved}) differs from run_state tracker resolved ({scored})")
        if shadow_fills is not None and shadow_fills != sum(1 for t in trades if t.mode == "hold"):
            notes.append(
                "latest_state shadow_fills is a runtime dashboard counter, not a closed-ledger count"
            )

    return notes


def validate_for_check(trades: list[Trade], errors: list[str]) -> list[str]:
    check_errors = list(errors)
    for t in trades:
        if t.mode not in ALL_KNOWN_MODES:
            check_errors.append(f"unknown mode after normalization: {t.mode}")
        if not math.isfinite(t.pnl):
            check_errors.append(f"non-finite pnl after normalization: {t}")
    return check_errors


def fmt_money(value: float) -> str:
    return f"{value:,.2f}"


def fmt_pct(value: float) -> str:
    return f"{100.0 * value:.1f}%"


def fmt_p(value: float) -> str:
    if math.isnan(value):
        return "n/a"
    if value < 0.001:
        return "<0.001"
    return f"{value:.3f}"


def markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |"]
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def strongest_current(stats: list[StrategyStats]) -> list[StrategyStats]:
    current = [s for s in stats if s.mode in CURRENT_MODES and s.windows >= 20]
    current.sort(key=lambda s: (s.holm_p_value if not math.isnan(s.holm_p_value) else 1.0, -s.mean_pnl))
    return current


def report_text(
    data_dir: Path,
    trades: list[Trade],
    history: list[Any],
    run_state: dict[str, Any],
    latest_state: dict[str, Any],
    stats: list[StrategyStats],
    forecast: list[ForecastStats],
    quality_notes: list[str],
) -> str:
    modes = collections.Counter(t.mode for t in trades)
    strategies = collections.Counter(t.strategy for t in trades)
    windows = sorted({t.t0_millis for t in trades})
    current_windows = sorted({t.t0_millis for t in trades if t.mode in CURRENT_MODES})
    legacy_row_count = sum(1 for t in trades if t.legacy)
    scored = run_state.get("tracker", {}).get("resolved", "n/a")
    latest_resolved = latest_state.get("resolved_windows", "n/a") if isinstance(latest_state, dict) else "n/a"

    leaders = strongest_current(stats)
    significant = [
        s for s in leaders if s.boot_ci_low > 0.0 and not math.isnan(s.holm_p_value) and s.holm_p_value < 0.05
    ]
    provisional = [
        s for s in leaders if s.mean_pnl > 0.0 and s.boot_ci_low <= 0.0
    ]
    if significant:
        conclusion = (
            "The shadow ledger contains statistically positive current-strategy cells after "
            "window-level bootstrap and Holm correction, but the conclusion remains research-only "
            "because the data mixes strategy revisions and the forecast scoreboard has only "
            f"{scored} resolved scoring windows."
        )
    elif provisional:
        conclusion = (
            "The current strategy is profitable in this sample, but not proven as a statistically "
            "reliable edge after conservative window-level uncertainty treatment. Treat it as "
            "promising forward-test evidence, not a deployment claim."
        )
    else:
        conclusion = (
            "The current strategy is not statistically proven from the available data. Positive "
            "cumulative PnL exists in places, but sample size, dependence, and legacy contamination "
            "prevent a strong edge claim."
        )

    current_stats = [s for s in stats if s.mode in CURRENT_MODES]
    current_stats.sort(key=lambda s: (s.mode, -s.total_pnl))
    leader_rows = []
    for s in current_stats:
        leader_rows.append(
            [
                s.mode,
                s.strategy,
                str(s.windows),
                str(s.raw_trades),
                fmt_money(s.total_pnl),
                fmt_money(s.mean_pnl),
                f"[{fmt_money(s.boot_ci_low)}, {fmt_money(s.boot_ci_high)}]",
                fmt_p(s.t_p_value),
                fmt_p(s.holm_p_value),
                fmt_money(s.max_drawdown),
                f"{s.approx_roi_pct:.1f}%",
            ]
        )

    forecast_rows = []
    for f in forecast:
        forecast_rows.append(
            [
                f.name,
                str(f.n),
                f"{f.brier:.5f}",
                f"{f.log_loss:.5f}",
                fmt_pct(f.directional_accuracy),
                f"{f.mean_pred:.3f}",
                f"{f.up_rate:.3f}",
                f"{f.brier_vs_climatology:+.5f}",
                f"{f.logloss_vs_climatology:+.5f}",
            ]
        )

    legacy_stats = [s for s in stats if s.mode in LEGACY_MODES]
    legacy_table_rows = [
        [
            s.mode,
            s.strategy,
            str(s.windows),
            fmt_money(s.total_pnl),
            fmt_money(s.mean_pnl),
            fmt_p(s.t_p_value),
        ]
        for s in legacy_stats
    ]

    timing = [t.entry_secs_remaining for t in trades if t.entry_secs_remaining is not None]
    timing_text = "No hold entry timing was parsed."
    if timing:
        timing_sorted = sorted(timing)
        timing_text = (
            f"Hold entries span t-{min(timing_sorted)}s to t-{max(timing_sorted)}s, "
            f"median t-{int(statistics.median(timing_sorted))}s, "
            f"p25 t-{int(quantile(timing_sorted, 0.25))}s, "
            f"p75 t-{int(quantile(timing_sorted, 0.75))}s."
        )

    best_forecast = forecast[0] if forecast else None
    calibration_section = ""
    if best_forecast:
        calibration_rows = [
            [
                f"{row['lo']:.1f}-{row['hi']:.1f}",
                str(int(row["n"])),
                f"{row['avg_pred']:.3f}",
                f"{row['observed']:.3f}",
            ]
            for row in best_forecast.calibration_bins
        ]
        calibration_section = "\n".join(
            [
                f"Best Brier model calibration bins: `{best_forecast.name}`.",
                "",
                markdown_table(["Pred bin", "N", "Avg pred", "Observed up"], calibration_rows),
            ]
        )

    quality_bullets = "\n".join(f"- {note}" for note in quality_notes) if quality_notes else "- No material data-quality warnings."
    mode_summary = ", ".join(f"{mode}={count}" for mode, count in sorted(modes.items()))
    strategy_summary = ", ".join(f"{name}={count}" for name, count in strategies.most_common())
    first_t0 = windows[0] if windows else "n/a"
    last_t0 = windows[-1] if windows else "n/a"

    return f"""# BTC 5m Polymarket Strategy Audit - 2026-06-23

## Executive Conclusion

{conclusion}

The main positive signal is not the dashboard headline alone; it is the repeated positive window-level PnL across several current modes and models. The main weakness is evidentiary: only {scored} scored forecast windows are available in `run_state.json`, while the trade ledger spans {len(windows)} windows and includes {legacy_row_count} legacy `maker` rows from a removed strategy. The right next decision is to freeze one candidate strategy definition and run a forward-only holdout before drawing a production conclusion.

## Data Snapshot

- Source directory: `{data_dir}`
- Trade rows: {len(trades)}
- Distinct trade windows: {len(windows)} total, {len(current_windows)} current-mode windows
- Window range: `{first_t0}` to `{last_t0}`
- Mode rows: {mode_summary}
- Strategy rows: {strategy_summary}
- Forecast scoreboard windows: {scored}; latest dashboard resolved windows: {latest_resolved}
- History points loaded: {len(history) if isinstance(history, list) else 0}

## Strategy Mechanics Audited

- `crates/app/src/main.rs` builds base models (`black_scholes`, `student_t`, `sharp`, optional `cos_vg`) and appends derived `calibrated`, `blend`, and `blend_dyn` forecasts. Current paper trading tracks each model as its own strategy rather than one monolithic portfolio.
- `crates/app/src/resolution.rs` implements hold-to-resolution and active intraday paper strategies. Hold opens at most one position per model/window when edge clears the adaptive threshold. Active moves a signed YES-equivalent position and requires entry edge to cover the spread round trip.
- `hold_open_reprice_sharp` is a shadow-only derived hold candidate: it reuses the `sharp` forecast but can only enter in the first minute after market open (`seconds_remaining >= 240`). It is designed to test whether the observed PnL comes from early-window under-reaction rather than generic hold behavior.
- `crates/app/src/convergence.rs` implements fair-value convergence: enter when model fair differs from market mid by enough, exit when the market converges, otherwise settle at resolution.
- `crates/app/src/adaptive.rs` learns a per-window typical maximum edge and sets the acting threshold as a clamped fraction of that value.
- `crates/pricing/src/platt.rs` performs online Platt scaling of `sharp`; `crates/pricing/src/weights.rs` performs online dynamic model averaging for `blend_dyn`.
- `crates/execution/src/risk.rs` blocks dry-run decisions on stale feeds/books, pin-window timing, model disagreement, spread, depth, exposure, notional, order-rate, and loss limits.
- The dashboard `shadow_fills`/PnL should not be read as one capital-allocated portfolio. It aggregates multiple model variants that often trade the same window, so statistical tests must use window-level blocks.

{timing_text}

## Data Quality Findings

{quality_bullets}

These findings do not make the ledger useless, but they do mean the report separates current modes (`hold`, `active`, `conv`) from legacy `maker` and treats model variants as correlated observations.

## Current Strategy Statistics

Window-level PnL is the independent unit. `Raw trades` is shown for traceability, but p-values and confidence intervals use one PnL observation per `(mode, strategy, window)`. Approx ROI assumes the configured `$10` paper budget per window-equivalent exposure.

{markdown_table(["Mode", "Strategy", "Windows", "Raw trades", "Total PnL", "Mean/window", "Boot 95% CI", "p mean", "Holm p", "Max DD", "Approx ROI"], leader_rows)}

Interpretation:

- Positive totals are broad, especially for `hold` and `active`, but the table is not a portfolio result because strategies are highly correlated and were run simultaneously on the same markets.
- A strategy cell is stronger evidence when its bootstrap CI is entirely above zero and its Holm-adjusted p-value remains below 0.05.
- Large positive max trades and positive skew mean a few big resolution wins contribute meaningfully; trimmed means in the script output should be checked before increasing size.

## Forecast Quality

Forecast scores come from `shadow-data/run_state.json`, not the trade ledger. Lower Brier/log-loss is better. `vs climatology` is positive when the model beats a constant forecast equal to the observed up-rate.

{markdown_table(["Model", "N", "Brier", "Log loss", "Direction acc", "Mean p", "Up rate", "Brier vs clim", "LL vs clim"], forecast_rows)}

{calibration_section}

The forecast layer looks directionally strong in-sample, but {scored} windows is a small sample for seven closely related models. The ranking currently favors the sharpened/calibrated family, which matches the code thesis that this market under-reacts, but it still needs forward-only confirmation.

## Legacy Strategy Evidence

The `maker` rows are historical and should not be included in current strategy claims.

{markdown_table(["Mode", "Strategy", "Windows", "Total PnL", "Mean/window", "p mean"], legacy_table_rows) if legacy_table_rows else "No legacy rows found."}

## Issues And Caveats

- Sample size is still thin for statistical proof, especially for forecast calibration: {scored} scored windows is not enough to trust subtle model ranking.
- Model variants are correlated because they trade the same 5-minute windows; naive per-trade significance overstates confidence.
- The ledger mixes code generations. `maker` is present historically but current code says market-making was removed as non-viable.
- The script assumes the ledger PnL already reflects the current paper fill model. It does not reconstruct order book depth, queue priority, latency, or fees from raw market data.
- Convergence intraday close rows use `outcome_up=false` as a placeholder before final settlement; PnL/won are usable, but outcome labels are not.
- Shadow execution is taker-style and optimistic relative to actual execution: no failed posts, queue movement between decision and order, API latency, or external cancellation/reconciliation errors are represented.
- Adaptive threshold and online Platt/DMA learning can data-snoop if tuned on the same run used to judge significance.
- Dashboard totals can be misleading if read as deployable portfolio PnL because multiple strategies are simultaneous alternatives, not capital-allocated decisions.

## Recommended Improvements

1. Freeze one candidate for the next run, preferably the best current-mode/model pair after excluding legacy rows and checking trimmed mean, then evaluate only that candidate forward.
2. Split ledgers by strategy version or add a `strategy_version` field so old `maker` and parameter revisions cannot contaminate future audits.
3. Add a market-implied baseline: compare each model forecast and trade against best bid/ask, mid, and a no-trade baseline at the same decision timestamp.
4. Add cost stress tests to the analysis: subtract fee/slippage buffers of 1c, 2c, and 5c per contract-equivalent and re-run significance.
5. Report portfolio-level results separately from model-cell results. A deployed system can only allocate finite capital across variants.
6. Promote window-level confidence metrics to the dashboard: scored windows, best candidate CI, current drawdown, and whether the latest run is out-of-sample.
7. Add a replay/backtest fixture with raw observations so the paper trackers can be rerun from source events rather than only summarized ledgers.
8. Keep the project paper-only and require a forward holdout before making stronger performance claims.

## Reproducibility

Commands:

```bash
python3 -m py_compile scripts/analyze_shadow_strategy.py
python3 scripts/analyze_shadow_strategy.py --check --data shadow-data
python3 scripts/analyze_shadow_strategy.py --data shadow-data --report docs/strategy-analysis-2026-06-23.md
```

Assumptions:

- `shadow-data/trades.json` is the source of truth for closed trade PnL.
- `shadow-data/run_state.json` is the source of truth for forecast Brier/log-loss boards.
- One `(mode, strategy, t0_millis)` PnL block is the statistical unit.
- Legacy `maker` rows are excluded from current-strategy conclusions.
- Approx ROI uses the default configured paper budget of `${DEFAULT_BUDGET_USD:.0f}` because the ledger does not store per-row notional.
"""


def print_summary(stats: list[StrategyStats], forecast: list[ForecastStats], quality_notes: list[str]) -> None:
    print("Data-quality notes:")
    for note in quality_notes:
        print(f"- {note}")
    print()
    print("Top current strategy cells by total PnL:")
    current = [s for s in stats if s.mode in CURRENT_MODES]
    current.sort(key=lambda s: s.total_pnl, reverse=True)
    for s in current[:10]:
        print(
            f"{s.mode:6s} {s.strategy:14s} windows={s.windows:3d} "
            f"total={s.total_pnl:9.2f} mean={s.mean_pnl:7.2f} "
            f"ci=[{s.boot_ci_low:6.2f},{s.boot_ci_high:6.2f}] "
            f"p={fmt_p(s.t_p_value)} holm={fmt_p(s.holm_p_value)}"
        )
    print()
    print("Forecast ranking:")
    for f in forecast:
        print(
            f"{f.name:14s} n={f.n:3d} brier={f.brier:.5f} "
            f"logloss={f.log_loss:.5f} acc={fmt_pct(f.directional_accuracy)}"
        )


def json_summary(
    data_dir: Path,
    trades: list[Trade],
    stats: list[StrategyStats],
    forecast: list[ForecastStats],
    quality_notes: list[str],
) -> dict[str, Any]:
    """Stable, machine-readable subset used by the portfolio replay demo."""
    def canonicalize(value: Any) -> Any:
        # libm can differ by a few ULPs across macOS and Linux. Twelve decimal
        # places is well beyond the UI/report precision and keeps CI byte-stable.
        if isinstance(value, float):
            return round(value, 12)
        if isinstance(value, list):
            return [canonicalize(item) for item in value]
        if isinstance(value, dict):
            return {key: canonicalize(item) for key, item in value.items()}
        return value

    current = [
        canonicalize(dataclasses.asdict(row))
        for row in stats
        if row.mode in CURRENT_MODES
    ]
    current.sort(key=lambda row: (row["mode"], -row["total_pnl"]))
    return {
        "schema_version": 1,
        "source": str(data_dir),
        "trade_rows": len(trades),
        "windows": len({trade.t0_millis for trade in trades}),
        "current_windows": len(
            {trade.t0_millis for trade in trades if trade.mode in CURRENT_MODES}
        ),
        "quality_notes": quality_notes,
        "strategies": current,
        "forecast_models": [canonicalize(dataclasses.asdict(row)) for row in forecast],
        "disclaimer": (
            "Historical shadow-run analysis, not a return claim. Strategy cells are "
            "correlated alternatives and are not one deployable portfolio."
        ),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="shadow-data", type=Path, help="directory containing shadow JSON files")
    parser.add_argument("--report", type=Path, help="write Markdown report to this path")
    parser.add_argument("--json", type=Path, help="write a machine-readable summary to this path")
    parser.add_argument("--check", action="store_true", help="validate input shape and exit nonzero on malformed data")
    args = parser.parse_args(argv)

    data_dir = args.data
    trades_raw = load_json(data_dir / "trades.json", [])
    history = load_json(data_dir / "history.json", [])
    run_state = load_json(data_dir / "run_state.json", {})
    latest_state = load_json(data_dir / "latest_state.json", {})

    trades, errors, normalize_warnings = normalize_trades(trades_raw)
    check_errors = validate_for_check(trades, errors)
    quality_notes = data_quality(trades, history, run_state, latest_state, normalize_warnings)

    if args.check:
        if check_errors:
            for error in check_errors:
                print(f"ERROR: {error}", file=sys.stderr)
            return 1
        print(
            f"OK: {len(trades)} trades, {len({t.t0_millis for t in trades})} windows, "
            f"modes={dict(sorted(collections.Counter(t.mode for t in trades).items()))}"
        )
        for note in quality_notes:
            print(f"NOTE: {note}")
        return 0

    if check_errors:
        for error in check_errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    stats = compute_strategy_stats(trades)
    forecast = compute_forecast_stats(run_state)

    if args.report:
        text = report_text(data_dir, trades, history, run_state, latest_state, stats, forecast, quality_notes)
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text, encoding="utf-8")
        print(f"wrote {args.report}")
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                json_summary(data_dir, trades, stats, forecast, quality_notes),
                indent=2,
                allow_nan=False,
            )
            + "\n",
            encoding="utf-8",
        )
        print(f"wrote {args.json}")
    if not args.report and not args.json:
        print_summary(stats, forecast, quality_notes)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
