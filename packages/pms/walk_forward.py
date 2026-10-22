"""Predeclared chronological selection and independently reset OOS replay.

Legacy result value objects remain readable; new computations use the matrix engine.
"""

from __future__ import annotations

import hashlib
import json
import math
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from itertools import product
from pathlib import Path
from statistics import mean
from uuid import uuid4

from packages.pms.backtest_matrix import Case, Cell, MatrixRunner, fingerprint, plain
from packages.pms.research import ResearchVariant

VERSION = "WALK_FORWARD_v2.0"
FINAL_START = date(2025, 1, 1)


@dataclass(frozen=True)
class WalkForwardWindow:
    name: str
    train_start: date
    train_end: date
    test_start: date
    test_end: date
    kind: str = "RETROSPECTIVE"

    def __post_init__(self):
        if (
            not self.name
            or not self.train_start <= self.train_end < self.test_start <= self.test_end
            or self.test_end >= date.today()
        ):
            raise ValueError(
                "Ordered historical train/test windows without overlap required"
            )
        if self.kind not in {"RETROSPECTIVE", "FINAL_HOLDOUT"}:
            raise ValueError("Unknown window kind")
        if self.train_end >= FINAL_START:
            raise ValueError("Training must end before the reserved 2025 holdout")
        if (self.test_start >= FINAL_START) != (self.kind == "FINAL_HOLDOUT"):
            raise ValueError("2025 onward must be explicitly labelled FINAL_HOLDOUT")
        if self.kind == "RETROSPECTIVE" and self.test_end >= FINAL_START:
            raise ValueError("Development tests cannot cross the holdout boundary")


DEFAULT_WINDOWS = (
    WalkForwardWindow(
        "WF_2020",
        date(2018, 1, 1),
        date(2019, 12, 31),
        date(2020, 1, 1),
        date(2020, 12, 31),
    ),
    WalkForwardWindow(
        "WF_2021_2022",
        date(2018, 1, 1),
        date(2020, 12, 31),
        date(2021, 1, 1),
        date(2022, 12, 31),
    ),
    WalkForwardWindow(
        "WF_2023_2024",
        date(2018, 1, 1),
        date(2022, 12, 31),
        date(2023, 1, 1),
        date(2024, 12, 31),
    ),
)
FINAL_WINDOW = WalkForwardWindow(
    "FINAL_2025",
    date(2018, 1, 1),
    date(2024, 12, 31),
    FINAL_START,
    date(2025, 12, 31),
    "FINAL_HOLDOUT",
)


@dataclass(frozen=True)
class ReplayPeriod:
    name: str
    start: date
    end: date
    kind: str

    def __post_init__(self):
        if not self.name or self.end < self.start or self.end >= date.today():
            raise ValueError("Historical replay period required")


@dataclass(frozen=True)
class SelectionPolicy:
    min_closed_trades: int = 24
    objective: str = "CAGR_MINUS_DRAWDOWN_v1"

    def __post_init__(self):
        if (
            type(self.min_closed_trades) is not int
            or self.min_closed_trades < 1
            or self.objective != "CAGR_MINUS_DRAWDOWN_v1"
        ):
            raise ValueError(
                "Fixed objective and positive training sample minimum required"
            )


@dataclass(frozen=True)
class SelectedParameters:
    adr: float
    rally: float
    risk: float
    volume_ratio: float
    training_score: float


@dataclass(frozen=True)
class WalkForwardResult:
    window: WalkForwardWindow
    parameters: SelectedParameters
    train_metrics: dict
    test_metrics: dict


def summarize_walk_forward(results):
    rows = list(results)
    returns = [float(r.test_metrics["total_return_pct"]) for r in rows]
    return dict(
        windows=len(rows),
        positive_oos_windows=sum(x > 0 for x in returns),
        average_oos_return_pct=mean(returns) if returns else None,
        worst_oos_drawdown_pct=max(
            (float(r.test_metrics["max_drawdown_pct"]) for r in rows), default=None
        ),
    )


def candidate_cases(base, *, quick=False):
    # Risk stays at the approved base value: this phase does not optimize leverage.
    values = product(
        (2.5, 3.0) if quick else (2.5, 3.0, 4.0),
        (12.0, 15.0) if quick else (12.0, 15.0, 18.0),
        (1.5,) if quick else (1.25, 1.5, 2.0),
    )
    return tuple(
        Case(
            f"ADR_{adr}_RALLY_{rally}_VOL_{volume}",
            ResearchVariant("B_A_C_E"),
            {
                "technical": {
                    "min_adr_20d_pct": adr,
                    "rally_min_pct": rally,
                    "rally_min_volume_ratio": volume,
                }
            },
            family="WALK_FORWARD",
        )
        for adr, rally, volume in values
    )


def validate_plan(windows, candidates):
    if not windows or not candidates or len({w.name for w in windows}) != len(windows):
        raise ValueError("Unique nonempty windows and candidate grid required")
    if len({fingerprint(asdict(c)) for c in candidates}) != len(candidates):
        raise ValueError("Duplicate candidates")
    if any(a.test_end >= b.test_start for a, b in zip(windows, windows[1:])):
        raise ValueError("Test windows must be chronological and disjoint")
    for c in candidates:
        if c.seed is not None or c.variant != ResearchVariant("B_A_C_E"):
            raise ValueError("Use one predeclared deterministic full strategy")
        if set(c.overrides) != {"technical"} or not set(c.overrides["technical"]) <= {
            "min_adr_20d_pct",
            "rally_min_pct",
            "rally_min_volume_ratio",
        }:
            raise ValueError("Only predeclared technical selection parameters may vary")
        if any(
            not isinstance(v, (int, float))
            or isinstance(v, bool)
            or not math.isfinite(v)
            or v <= 0
            for v in c.overrides["technical"].values()
        ):
            raise ValueError("Finite positive candidate settings required")


def phase_cells(window, candidates, phase):
    start, end = (
        (window.train_start, window.train_end)
        if phase == "TRAIN"
        else (window.test_start, window.test_end)
    )
    period = ReplayPeriod(window.name + "_" + phase, start, end, phase)
    return tuple(Cell(period, c) for c in candidates)


def select_candidate(rows, policy):
    if not rows or any(r["status"] not in {"COMPLETED", "NO_TRADES"} for r in rows):
        return None, "training_grid_incomplete"
    qualified = []
    for row in rows:
        m = row["metrics"]
        if m["closed_trades"] < policy.min_closed_trades:
            continue
        cagr, dd = m.get("cagr_pct"), m.get("max_drawdown_pct")
        if any(v is None or not math.isfinite(v) for v in (cagr, dd)):
            continue
        qualified.append((float(cagr) - float(dd), row["key"], row))
    if not qualified:
        return None, "insufficient_training_sample"
    score, _, winner = sorted(qualified, key=lambda x: (-x[0], x[1]))[0]
    return (
        dict(
            candidate=deepcopy(winner["case"]),
            training_key=winner["key"],
            training_evidence_hash=winner["evidence_hash"],
            score=score,
            objective=policy.objective,
            tie_break="ascending cell SHA256 key",
        ),
        None,
    )


def _verify_file(path, evidence):
    target = (path.parent / evidence["path"]).resolve()
    if (
        not target.is_relative_to(path.parent.resolve())
        or not target.is_file()
        or hashlib.sha256(target.read_bytes()).hexdigest() != evidence["sha256"]
    ):
        raise ValueError("Walk-forward checkpoint evidence missing or changed")


class WalkForwardRunner:
    def __init__(
        self,
        base,
        windows,
        candidates,
        datasets,
        path,
        *,
        executor,
        gate,
        code_version,
        policy=None,
    ):
        self.base, self.windows, self.candidates = (
            deepcopy(base),
            tuple(windows),
            tuple(candidates),
        )
        validate_plan(self.windows, self.candidates)
        self.policy = policy or SelectionPolicy()
        self.datasets, self.path, self.executor, self.gate = (
            datasets,
            Path(path),
            executor,
            gate,
        )
        self.code_version = code_version
        plan = plain([asdict(w) for w in self.windows])
        self.identity = fingerprint(
            dict(
                version=VERSION,
                config=base,
                windows=plan,
                candidates=[asdict(c) for c in candidates],
                policy=asdict(self.policy),
                datasets=datasets,
                code=code_version,
            )
        )
        if self.path.exists():
            self.report = json.loads(self.path.read_text())
            if self.report.get("identity") != self.identity:
                raise ValueError(
                    "Walk-forward inputs/config/code/plan changed; use a new report path"
                )
            if self.report.get("checkpoint_hash") != fingerprint(
                {k: v for k, v in self.report.items() if k != "checkpoint_hash"}
            ):
                raise ValueError("Walk-forward checkpoint changed")
            if [r["window"] for r in self.report["folds"]] != plan:
                raise ValueError("Walk-forward window list changed")
            for row in self.report["folds"]:
                for phase in ("train_checkpoint", "test_checkpoint"):
                    if row.get(phase):
                        _verify_file(self.path, row[phase])
                        inner_path = self.path.parent / row[phase]["path"]
                        inner = json.loads(inner_path.read_text())
                        for cell in inner["cells"]:
                            if cell["status"] in {"COMPLETED", "NO_TRADES"}:
                                if cell.get("evidence_hash") != fingerprint(
                                    {
                                        k: v
                                        for k, v in cell.items()
                                        if k != "evidence_hash"
                                    }
                                ):
                                    raise ValueError(
                                        "Training/test evidence checksum changed"
                                    )
                                diag = cell.get("diagnostics", {})
                                if diag.get("evidence_file"):
                                    _verify_file(
                                        inner_path,
                                        dict(
                                            path=diag["evidence_file"],
                                            sha256=diag.get("evidence_sha256"),
                                        ),
                                    )
        else:
            self.report = dict(
                run_id=str(uuid4()),
                identity=self.identity,
                version=VERSION,
                code_version=code_version,
                created_at=datetime.now(timezone.utc).isoformat(),
                config=deepcopy(base),
                datasets=datasets,
                policy=asdict(self.policy),
                candidates=plain([asdict(c) for c in candidates]),
                scope="RESEARCH_ONLY",
                design_scope=(
                    "DEFAULT_GRID"
                    if plain([asdict(c) for c in candidates])
                    == plain([asdict(c) for c in candidate_cases(base)])
                    else "CUSTOM_OR_REDUCED_GRID"
                ),
                folds=[dict(window=w, status="PENDING") for w in plan],
            )

    def save(self):
        rows = self.report["folds"]
        completed = [r for r in rows if r["status"] in {"COMPLETED", "NO_TRADES"}]
        # Fold accounts reset. Never present their average as a compounded portfolio.
        self.report["summary"] = dict(
            status="COMPLETED" if len(completed) == len(rows) else "INCOMPLETE",
            folds=len(rows),
            computed_folds=len(completed),
            acceptance="NOT_CERTIFIED",
            performance_pass=None,
            positive_oos_folds=sum(
                r["test_metrics"]["total_return_pct"] > 0 for r in completed
            ),
            mean_oos_return_pct=(
                mean(r["test_metrics"]["total_return_pct"] for r in completed)
                if completed
                else None
            ),
            worst_oos_drawdown_pct=max(
                (r["test_metrics"]["max_drawdown_pct"] for r in completed), default=None
            ),
            final_holdout_included=any(w.kind == "FINAL_HOLDOUT" for w in self.windows),
            design_scope=self.report["design_scope"],
            note="Independent reset fold accounts; returns are not compounded. Pre-2025 tests are retrospective; pristine holdout status is not certified.",
        )
        self.report["checkpoint_hash"] = fingerprint(
            {k: v for k, v in self.report.items() if k != "checkpoint_hash"}
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(plain(self.report), sort_keys=True, indent=2, allow_nan=False)
        )
        tmp.replace(self.path)

    def run(self, *, max_cells=None, on_progress=None):
        if max_cells is not None and max_cells < 1:
            raise ValueError("max_cells must be positive")
        attempts = 0
        folder = self.path.parent / (self.path.stem + "_folds")
        folder.mkdir(parents=True, exist_ok=True)
        for index, (window, row) in enumerate(zip(self.windows, self.report["folds"])):
            if row["status"] in {"COMPLETED", "NO_TRADES"}:
                continue
            if max_cells is not None and attempts >= max_cells:
                break

            def checkpoint(phase, cases):
                nonlocal attempts
                cells = phase_cells(window, cases, phase)
                path = folder / f"{index}_{phase.lower()}.json"
                dataset = self.datasets[window.name][phase]
                runner = MatrixRunner(
                    self.base,
                    cells,
                    dataset,
                    path,
                    executor=lambda cell, cfg: self.executor(window, phase, cell, cfg),
                    gate=lambda cell, cfg: self.gate(window, phase, cell, cfg),
                    code_version=self.code_version,
                    purpose="WALK_FORWARD_PHASE",
                )

                def progress(cell_row):
                    nonlocal attempts
                    if cell_row["status"] == "RUNNING":
                        attempts += 1
                    payload = path.read_bytes()
                    row[phase.lower() + "_checkpoint"] = dict(
                        path=str(path.relative_to(self.path.parent)),
                        sha256=hashlib.sha256(payload).hexdigest(),
                    )
                    row["status"] = phase + "_RUNNING"
                    self.save()
                    if on_progress:
                        on_progress(self.report)

                remaining = None if max_cells is None else max_cells - attempts
                if remaining == 0:
                    return runner.report.get("cells", [])
                return runner.run(max_cells=remaining, on_progress=progress)["cells"]

            train = checkpoint("TRAIN", self.candidates)
            selection, reason = select_candidate(train, self.policy)
            row["training_blockers"] = sorted(
                {reason for r in train for reason in r.get("blockers", [])}
            )
            row["training_errors"] = sorted(
                {r["error_type"] for r in train if r.get("error_type")}
            )
            row["training_counts"] = {
                s: sum(r["status"] == s for r in train)
                for s in (
                    "COMPLETED",
                    "NO_TRADES",
                    "BLOCKED",
                    "FAILED",
                    "PENDING",
                    "RUNNING",
                )
            }
            if reason:
                row["status"] = (
                    "BLOCKED"
                    if not any(r["status"] in {"PENDING", "RUNNING"} for r in train)
                    else "TRAIN_PENDING"
                )
                row["blockers"] = [reason]
                self.save()
                break  # Never skip an unassessable earlier fold to reach a later holdout.
            candidate = next(
                c for c in self.candidates if plain(asdict(c)) == selection["candidate"]
            )
            frozen_config = candidate.config(self.base)
            freeze = {
                **selection,
                "config_hash": fingerprint(frozen_config),
                "train_dataset_fingerprint": self.datasets[window.name]["TRAIN"][
                    "fingerprint"
                ],
                "selected_on": "TRAIN_ONLY",
                "policy": asdict(self.policy),
            }
            if row.get("selection") and row["selection"] != freeze:
                raise ValueError("Frozen selection no longer matches training evidence")
            row["selection"] = freeze
            row["train_metrics"] = next(
                r["metrics"] for r in train if r["key"] == selection["training_key"]
            )
            row["status"] = "FROZEN"
            row["blockers"] = []
            self.save()  # Durable freeze before any OOS data evaluation.
            if on_progress:
                on_progress(self.report)
            if max_cells is not None and attempts >= max_cells:
                break
            test = checkpoint("TEST", (candidate,))[0]
            row["status"] = (
                test["status"]
                if test["status"] in {"COMPLETED", "NO_TRADES", "BLOCKED", "FAILED"}
                else "TEST_PENDING"
            )
            row["blockers"] = test.get("blockers", [])
            if test.get("error_type"):
                row["error_type"] = test["error_type"]
            if test["status"] in {"COMPLETED", "NO_TRADES"}:
                row.pop("error_type", None)
                row["test_metrics"] = test["metrics"]
                row["test_evidence_hash"] = test["evidence_hash"]
                row["generalization_gap_pct"] = (
                    test["metrics"]["cagr_pct"] - row["train_metrics"]["cagr_pct"]
                )
            self.save()
            if on_progress:
                on_progress(self.report)
            if row["status"] not in {"COMPLETED", "NO_TRADES"}:
                break
        self.save()
        return self.report
