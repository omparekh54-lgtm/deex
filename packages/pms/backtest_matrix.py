"""Predeclared, resumable research comparisons; completion is not acceptance."""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from itertools import product
from pathlib import Path
from statistics import mean, pstdev
from uuid import uuid4

from packages.pms.research import ResearchVariant

MATRIX_VERSION = "BACKTEST_MATRIX_v1.0"
DEVELOPMENT_END = date(2024, 12, 31)
RANDOM_SEEDS = (11, 29, 47, 71, 97)


def plain(value):
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite research evidence")
    return value


def fingerprint(value):
    return hashlib.sha256(
        json.dumps(
            plain(value), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


@dataclass(frozen=True)
class Period:
    name: str
    start: date
    end: date
    kind: str = "DEVELOPMENT"

    def __post_init__(self):
        if not self.name or self.end < self.start or self.end > DEVELOPMENT_END:
            raise ValueError(
                "Matrix periods must be ordered and end by 2024-12-31; reserve later data for OOS"
            )


@dataclass(frozen=True)
class Case:
    name: str
    variant: ResearchVariant
    overrides: dict = field(default_factory=dict)
    seed: int | None = None
    family: str = "CORE"

    def config(self, base):
        cfg = deepcopy(base)
        for group, settings in self.overrides.items():
            cfg[group].update(deepcopy(settings))
        cfg["backtest"]["matrix_research_only"] = True
        return cfg


@dataclass(frozen=True)
class Cell:
    period: Period
    case: Case

    @property
    def key(self):
        return fingerprint(asdict(self))[:24]


def default_periods():
    return (
        Period("DEVELOPMENT_2018_2024", date(2018, 1, 1), DEVELOPMENT_END),
        *(
            Period(str(y), date(y, 1, 1), date(y, 12, 31), "YEAR")
            for y in range(2018, 2025)
        ),
        Period("MIDSMALL_FALL", date(2018, 1, 1), date(2019, 12, 31), "STRESS"),
        Period("COVID", date(2020, 2, 1), date(2020, 5, 31), "STRESS"),
        Period("2022_WEAK", date(2022, 1, 1), date(2022, 12, 31), "STRESS"),
    )


def core_cases():
    variants = (
        ResearchVariant(
            "B_ONLY", use_module_a=False, use_module_c=False, use_module_e=False
        ),
        ResearchVariant("B_A", use_module_c=False, use_module_e=False),
        ResearchVariant(
            "EARNINGS_SETUP2", use_module_c=False, use_module_e=False, setup_2=True
        ),
        ResearchVariant("B_A_C", use_module_e=False),
        ResearchVariant("B_A_C_E"),
    )
    return tuple(
        Case(
            v.name + ("_REENTRY" if reentry else "_NO_REENTRY"),
            ResearchVariant(**{**asdict(v), "reentry": reentry}),
        )
        for v, reentry in product(variants, (True, False))
    )


def sensitivity_cases(base):
    full = ResearchVariant("B_A_C_E")
    out = []
    for adr, window, cap in product((2.5, 3.0, 4.0), (20, 50), (None, 8.0)):
        if (adr, window, cap) == (
            float(base["technical"]["min_adr_20d_pct"]),
            int(base["backtest"].get("research_adr_window", 20)),
            base["backtest"].get("research_adr_upper_cap"),
        ):
            continue  # Identical to the predeclared baseline.
        out.append(
            Case(
                f"ADR_{adr}_{window}_CAP_{cap}",
                full,
                {
                    "technical": {"min_adr_20d_pct": adr},
                    "backtest": {
                        "research_adr_window": window,
                        "research_adr_upper_cap": cap,
                    },
                },
                family="ADR",
            )
        )
    out.append(
        Case(
            (
                "LEG1_FIRST_CLOSE_ARMED"
                if base["backtest"].get("research_leg1_always_armed", False)
                else "LEG1_ALWAYS_ARMED"
            ),
            full,
            {
                "backtest": {
                    "research_leg1_always_armed": not base["backtest"].get(
                        "research_leg1_always_armed", False
                    )
                }
            },
            family="EXIT",
        )
    )
    out.extend(
        Case(f"C_RANDOM_{seed}", full, seed=seed, family="RANDOM")
        for seed in RANDOM_SEEDS
    )
    out.append(
        Case(
            "DOUBLE_FRICTION",
            full,
            {
                "backtest": {
                    key: 2 * float(base["backtest"][key])
                    for key in ("slippage_bps", "impact_bps")
                }
            },
            family="COST",
        )
    )
    return tuple(out)


def matrix_plan(base, periods=None, *, quick=False):
    periods = tuple(default_periods() if periods is None else periods)
    if not periods or len({p.name for p in periods}) != len(periods):
        raise ValueError("Nonempty, uniquely named matrix periods required")
    cases = core_cases()
    cells = [Cell(p, c) for p in periods for c in cases]
    if not quick:
        cells += [Cell(periods[0], c) for c in sensitivity_cases(base)]
    if len({c.key for c in cells}) != len(cells):
        raise ValueError("Duplicate matrix cell")
    return tuple(cells)


def round_trips(result):
    """One outcome per completed position cycle, including partials and costs."""
    entries = {
        (e.trade_date, e.instrument_id): e.payload
        for e in result.events
        if e.event_type.value == "ENTRY_FILL"
    }
    stream = [(f.trade_date, 1, i, "fill", f) for i, f in enumerate(result.fills)]
    stream += [
        (e.trade_date, 0, i, "action", e)
        for i, e in enumerate(result.events)
        if e.event_type.value == "CORPORATE_ACTION"
    ]
    active, closed = {}, []
    for day, _, _, kind, item in sorted(stream, key=lambda x: x[:3]):
        instrument = item.instrument_id
        if kind == "action":
            lot = active.get(instrument)
            if not lot:
                continue
            factor = item.payload.get("price_factor", 1)
            lot["quantity"] /= factor
            lot["income"] += float(item.payload.get("cash_income", 0))
            continue
        f = item
        if f.side == "BUY":
            if instrument in active:
                raise ValueError("Overlapping position cycles")
            active[instrument] = dict(
                entry_date=day,
                quantity=f.quantity,
                initial_quantity=f.quantity,
                capital=f.price * f.quantity + f.costs.total,
                invested=f.price * f.quantity,
                costs=f.costs.total,
                proceeds=0.0,
                income=0.0,
                risk=entries.get((day, instrument), {}).get("initial_risk"),
            )
        else:
            if (
                instrument not in active
                or f.quantity > active[instrument]["quantity"] + 1e-6
            ):
                raise ValueError("Exit without sufficient research position")
            lot = active[instrument]
            lot["quantity"] -= f.quantity
            lot["costs"] += f.costs.total
            lot["proceeds"] += f.quantity * f.price
            if abs(lot["quantity"]) < 1e-6:
                pnl = lot["proceeds"] + lot["income"] - lot["invested"] - lot["costs"]
                closed.append(
                    {
                        "instrument": instrument,
                        "entry_date": lot["entry_date"],
                        "exit_date": day,
                        "pnl": pnl,
                        "capital": lot["capital"],
                        "risk": lot["risk"],
                        "r_multiple": (
                            pnl / lot["risk"]
                            if lot["risk"] and lot["risk"] > 0
                            else None
                        ),
                        "holding_days": (day - lot["entry_date"]).days,
                    }
                )
                del active[instrument]
    return closed, active


def metrics(result, period):
    closed, active = round_trips(result)
    values = [
        float(result.initial_capital),
        *(float(v) for _, v in result.equity_curve),
    ]
    if not result.equity_curve or any(not math.isfinite(v) or v <= 0 for v in values):
        raise ValueError("Positive marked daily NAV evidence required")
    returns = [b / a - 1 for a, b in zip(values, values[1:])]
    years = max(1, (period.end - period.start).days + 1) / 365.25
    peak, drawdown = values[0], 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = max(drawdown, (1 - value / peak) * 100)
    winners = [t["pnl"] for t in closed if t["pnl"] > 0]
    losers = [t["pnl"] for t in closed if t["pnl"] < 0]
    stdev = pstdev(returns) if len(returns) > 1 else 0
    downside = math.sqrt(mean(min(v, 0) ** 2 for v in returns))
    cagr = ((values[-1] / values[0]) ** (1 / years) - 1) * 100
    return plain(
        dict(
            total_return_pct=(values[-1] / values[0] - 1) * 100,
            cagr_pct=cagr,
            max_drawdown_pct=drawdown,
            sharpe=mean(returns) / stdev * math.sqrt(252) if stdev else None,
            sortino=mean(returns) / downside * math.sqrt(252) if downside else None,
            calmar=cagr / drawdown if drawdown else None,
            risk_free_rate=0,
            ratio_basis="daily NAV returns; 252 sessions/year; zero risk-free assumption",
            profit_factor=sum(winners) / abs(sum(losers)) if losers else None,
            profit_factor_note="no losing closed cycles" if not losers else None,
            closed_trades=len(closed),
            fills=len(result.fills),
            open_positions=len(active),
            win_rate_pct=len(winners) / len(closed) * 100 if closed else None,
            expectancy_rupees=mean(t["pnl"] for t in closed) if closed else None,
            expectancy_r=(
                mean(t["r_multiple"] for t in closed)
                if closed and all(t["r_multiple"] is not None for t in closed)
                else None
            ),
            average_holding_days=(
                mean(t["holding_days"] for t in closed) if closed else None
            ),
            trades_per_year=len(closed) / years,
            initial_capital=values[0],
            ending_equity=values[-1],
            ending_cash=result.ending_cash,
            unrealized_pnl=result.unrealized_pnl,
            realized_gross_pnl=result.realized_gross_pnl,
            total_costs=result.total_costs,
            realized_net_pnl=result.realized_net_pnl,
            closed_by_year={
                str(y): sum(t["exit_date"].year == y for t in closed)
                for y in range(period.start.year, period.end.year + 1)
            },
        )
    )


def summarize(cells):
    counts = {
        s: sum(c["status"] == s for c in cells)
        for s in ("COMPLETED", "NO_TRADES", "BLOCKED", "FAILED", "PENDING", "RUNNING")
    }
    finished = counts["COMPLETED"] + counts["NO_TRADES"]
    comparisons = []
    for period in sorted({r["period"]["name"] for r in cells}):
        rows = {
            r["case"]["name"]: r
            for r in cells
            if r["period"]["name"] == period
            and r["status"] in {"COMPLETED", "NO_TRADES"}
        }
        for label, left, right in [
            ("E_ON_VS_OFF", "B_A_C_REENTRY", "B_A_C_E_REENTRY"),
            ("C_ON_VS_OFF", "B_A_REENTRY", "B_A_C_REENTRY"),
        ]:
            if left in rows and right in rows:
                a, b = rows[left]["metrics"], rows[right]["metrics"]
                comparisons.append(
                    dict(
                        period=period,
                        comparison=label,
                        baseline=left,
                        treatment=right,
                        return_delta_pct=b["total_return_pct"] - a["total_return_pct"],
                        drawdown_reduction_pct=a["max_drawdown_pct"]
                        - b["max_drawdown_pct"],
                        closed_trades=[a["closed_trades"], b["closed_trades"]],
                        performance_pass=None,
                    )
                )
    random_rows = [
        r
        for r in cells
        if r["case"]["family"] == "RANDOM" and r["status"] in {"COMPLETED", "NO_TRADES"}
    ]
    random_summary = dict(
        seeds_planned=sum(r["case"]["family"] == "RANDOM" for r in cells),
        seeds_computed=len(random_rows),
        average_return_pct=(
            mean(r["metrics"]["total_return_pct"] for r in random_rows)
            if random_rows
            else None
        ),
        selection="No seed is selected or promoted",
    )
    return dict(
        comparisons=comparisons,
        random_control=random_summary,
        counts=counts,
        cells=len(cells),
        computed_cells=finished,
        status="COMPLETED" if finished == len(cells) else "INCOMPLETE",
        acceptance="NOT_CERTIFIED",
        performance_pass=None,
        note="Overlapping periods and variants are independent experiments; never sum their returns or trade counts.",
    )


class MatrixRunner:
    """Checkpoint only completed cells; changed config/data cannot reuse evidence."""

    def __init__(
        self,
        base,
        cells,
        dataset,
        path,
        *,
        executor,
        gate,
        code_version,
        purpose="MATRIX",
    ):
        if purpose not in {"MATRIX", "WALK_FORWARD_PHASE"}:
            raise ValueError("Unknown research purpose")
        self.base, self.cells, self.dataset = deepcopy(base), tuple(cells), dataset
        if not self.cells:
            raise ValueError("Nonempty matrix required")
        self.path, self.executor, self.gate = Path(path), executor, gate
        plan = [plain(asdict(c)) for c in cells]
        self.identity = fingerprint(
            dict(
                version=MATRIX_VERSION,
                purpose=purpose,
                config=base,
                cells=plan,
                dataset=dataset,
                code=code_version,
            )
        )
        if self.path.exists():
            self.report = json.loads(self.path.read_text(encoding="utf-8"))
            if self.report.get("identity") != self.identity:
                raise ValueError(
                    "Checkpoint config, data, code or plan changed; use a new report path"
                )
            keys = [r.get("key") for r in self.report.get("cells", [])]
            if keys != [c.key for c in cells]:
                raise ValueError("Checkpoint cell list mismatch")
            for row in self.report["cells"]:
                if row["status"] in {"COMPLETED", "NO_TRADES"} and row.get(
                    "evidence_hash"
                ) != fingerprint(
                    {k: v for k, v in row.items() if k != "evidence_hash"}
                ):
                    raise ValueError("Checkpoint evidence checksum mismatch")
                evidence = row.get("diagnostics", {}).get("evidence_file")
                if row["status"] in {"COMPLETED", "NO_TRADES"} and evidence:
                    evidence_path = (self.path.parent / evidence).resolve()
                    if (
                        not evidence_path.is_relative_to(self.path.parent.resolve())
                        or not evidence_path.is_file()
                        or hashlib.sha256(evidence_path.read_bytes()).hexdigest()
                        != row["diagnostics"].get("evidence_sha256")
                    ):
                        raise ValueError("Missing or changed cell trade journal")
        else:
            self.report = dict(
                run_id=str(uuid4()),
                identity=self.identity,
                version=MATRIX_VERSION,
                code_version=code_version,
                created_at=datetime.now(timezone.utc).isoformat(),
                dataset=dataset,
                config_hash=fingerprint(base),
                config=deepcopy(base),
                scope="RESEARCH_ONLY",
                design_scope=(
                    "WALK_FORWARD_PHASE"
                    if purpose == "WALK_FORWARD_PHASE"
                    else (
                        "FULL_DEFAULT"
                        if [c.key for c in self.cells]
                        == [c.key for c in matrix_plan(base)]
                        else "REDUCED_OR_CUSTOM"
                    )
                ),
                holdout=(
                    "Walk-forward phase: training selects parameters; selected tests do not select or promote settings"
                    if purpose == "WALK_FORWARD_PHASE"
                    else "2025 onward is reserved; no parameter selection or production config updates"
                ),
                cells=[
                    dict(
                        key=c.key,
                        period=plain(asdict(c.period)),
                        case=plain(asdict(c.case)),
                        status="PENDING",
                    )
                    for c in cells
                ],
            )

    def save(self):
        self.report["summary"] = summarize(self.report["cells"])
        self.report["summary"]["design_scope"] = self.report.get(
            "design_scope", "REDUCED_OR_CUSTOM"
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(plain(self.report), indent=2, sort_keys=True, allow_nan=False),
            encoding="utf-8",
        )
        temporary.replace(self.path)

    def run(self, *, max_cells=None, on_progress=None):
        if max_cells is not None and max_cells < 1:
            raise ValueError("max_cells must be positive")
        attempts = 0
        for cell, row in zip(self.cells, self.report["cells"]):
            if row["status"] in {"COMPLETED", "NO_TRADES"}:
                continue
            if max_cells is not None and attempts >= max_cells:
                break
            attempts += 1
            row.pop("evidence_hash", None)
            row.pop("metrics", None)
            row.pop("error_type", None)
            row.pop("diagnostics", None)
            row.pop("trade_frequency_sufficient", None)
            row["status"] = "RUNNING"
            self.save()
            if on_progress:
                on_progress(row)
            try:
                cfg = cell.case.config(self.base)
                blockers = self.gate(cell, cfg)
                row["blockers"] = sorted(set(blockers))
                if blockers:
                    row["status"] = "BLOCKED"
                else:
                    result, diagnostics = self.executor(cell, cfg)
                    row["metrics"] = metrics(result, cell.period)
                    row["diagnostics"] = plain(diagnostics)
                    row["status"] = (
                        "COMPLETED" if row["metrics"]["closed_trades"] else "NO_TRADES"
                    )
                    row["trade_frequency_sufficient"] = (
                        row["metrics"]["trades_per_year"] >= 24
                    )
            except Exception as exc:
                from packages.pms.matrix_replay import ResearchDataGap

                row["status"] = (
                    "BLOCKED" if isinstance(exc, ResearchDataGap) else "FAILED"
                )
                if isinstance(exc, ResearchDataGap):
                    row["blockers"] = [str(exc)]
                row["error_type"] = type(exc).__name__
                # Do not serialize database URLs, SQL errors or credentials.
            row["evidence_hash"] = fingerprint(
                {k: v for k, v in row.items() if k != "evidence_hash"}
            )
            self.save()
            if on_progress:
                on_progress(row)
        self.save()
        return self.report
