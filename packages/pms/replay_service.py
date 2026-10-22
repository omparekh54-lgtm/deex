"""Common execution and read-only input preparation for research entry points."""
from __future__ import annotations

import math
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict
from datetime import date

from packages.pms.backtest_matrix import Cell, fingerprint
from packages.pms.matrix_data import (
    MatrixGates, code_fingerprint, consistent_inputs, input_manifest,
)
from packages.pms.matrix_replay import MatrixReplay, ResearchDataGap
from packages.pms.walk_forward import ReplayPeriod

ENGINE_VERSION = "ISOLATED_REPLAY_v2"


def execution_identity(cfg, case):
    return {
        "engine": ENGINE_VERSION,
        "strategy_version": cfg["version"],
        "config_fingerprint": fingerprint(cfg),
        "policy_fingerprint": fingerprint({
            k: cfg[k] for k in ("fundamental", "technical", "ranking", "risk", "backtest")
        }),
        "variant": asdict(case.variant),
        "selection_seed": case.seed,
    }


def execute_replay(snapshot, symbols, cfg, case, start, end):
    """Caller owns the consistent snapshot and dataset gates (matrix or session)."""
    config = deepcopy(cfg)
    config["backtest"]["matrix_research_only"] = True
    capital = config["backtest"]["initial_capital"]
    if not math.isfinite(float(capital)) or capital <= 0:
        raise ValueError("Initial capital must be finite and positive")
    if end < start or end >= date.today():
        raise ValueError("Ordered historical replay dates required")
    if case.variant.use_module_c and not case.variant.use_module_a:
        raise ValueError("Module C requires Module A eligibility")
    selected = sorted(set(symbols))
    if not selected:
        raise ResearchDataGap("historical_universe_empty")
    identity = execution_identity(config, case)
    result, diagnostics = MatrixReplay(snapshot, selected, config, case, start, end).run()
    diagnostics.update(identity)
    diagnostics["result_fingerprint"] = fingerprint(asdict(result))
    return result, diagnostics


class PreparedReplay:
    def __init__(self, snapshot, manifest, period):
        self.snapshot, self.manifest, self.period = snapshot, manifest, period
        self.symbols = [r["instrument_id"] for r in manifest["universe"]]
        self.gate = MatrixGates(snapshot, manifest)
        self.code_version = code_fingerprint()

    def run(self, cfg, case):
        blockers = self.gate(Cell(self.period, case), cfg)
        if blockers:
            raise ResearchDataGap(";".join(blockers))
        result, diagnostics = execute_replay(
            self.snapshot, self.symbols, cfg, case, self.period.start, self.period.end
        )
        diagnostics.update(
            dataset_fingerprint=self.manifest["fingerprint"],
            code_version=self.code_version,
            scope=self.manifest["scope"],
            acceptance="NOT_CERTIFIED",
            period_kind=self.period.kind,
        )
        return result, diagnostics


@contextmanager
def prepared_replay(engine, symbols, start, end, cases):
    # Single runs can inspect reserved dates but never claim out-of-sample acceptance.
    kind = "EXPLORATORY_HOLDOUT" if end >= date(2025, 1, 1) else "DEVELOPMENT"
    period = ReplayPeriod("CUSTOM", start, end, kind)
    cells = [Cell(period, case) for case in cases]
    selected = sorted({s.strip().upper() for s in symbols if s.strip()})
    if not selected:
        raise ValueError("At least one canonical symbol is required")
    if not cells:
        raise ValueError("At least one research case is required")
    with consistent_inputs(engine) as snapshot:
        manifest = input_manifest(snapshot, cells, selected)
        yield PreparedReplay(snapshot, manifest, period)
