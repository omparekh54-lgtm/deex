from __future__ import annotations

import json
from dataclasses import asdict
from uuid import UUID

from sqlalchemy import text

from packages.pms.stress_testing import StressResult
from packages.pms.walk_forward import WalkForwardResult


class ValidationPersistence:
    def __init__(self, engine):
        self.engine = engine

    def save_stress_run(self, run_id: UUID, *, variant: str, symbols: list[str], initial_capital: float | None, results: list[StressResult], summary: dict) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                insert into stress_test_runs(id, variant, symbols, initial_capital, summary)
                values (:id,:variant,cast(:symbols as jsonb),:capital,cast(:summary as jsonb))
            """), {"id": run_id, "variant": variant, "symbols": json.dumps(symbols), "capital": initial_capital, "summary": json.dumps(summary)})
            for result in results:
                conn.execute(text("""
                    insert into stress_test_results
                    (run_id, period_name, start_date, end_date, description, metrics)
                    values (:run_id,:name,:start,:end,:description,cast(:metrics as jsonb))
                """), {"run_id": run_id, "name": result.period.name, "start": result.period.start, "end": result.period.end,
                        "description": result.period.description, "metrics": json.dumps(result.metrics)})

    def save_walk_forward_run(self, run_id: UUID, *, variant: str, symbols: list[str], initial_capital: float | None, results: list[WalkForwardResult], summary: dict) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("""
                insert into walk_forward_runs(id, variant, symbols, initial_capital, summary)
                values (:id,:variant,cast(:symbols as jsonb),:capital,cast(:summary as jsonb))
            """), {"id": run_id, "variant": variant, "symbols": json.dumps(symbols), "capital": initial_capital, "summary": json.dumps(summary)})
            for result in results:
                w = result.window
                conn.execute(text("""
                    insert into walk_forward_results
                    (run_id, window_name, train_start, train_end, test_start, test_end,
                     selected_parameters, train_metrics, test_metrics)
                    values (:run_id,:name,:train_start,:train_end,:test_start,:test_end,
                            cast(:params as jsonb),cast(:train as jsonb),cast(:test as jsonb))
                """), {"run_id": run_id, "name": w.name, "train_start": w.train_start, "train_end": w.train_end,
                        "test_start": w.test_start, "test_end": w.test_end,
                        "params": json.dumps(asdict(result.parameters)),
                        "train": json.dumps(result.train_metrics), "test": json.dumps(result.test_metrics)})
