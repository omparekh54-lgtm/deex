"""Evidence adapters for the isolated walk-forward runner; no strategy state writes."""

from copy import deepcopy
from dataclasses import asdict
from pathlib import Path
import hashlib
import json
from sqlalchemy import text

from packages.pms.backtest_matrix import plain
from packages.pms.matrix_data import input_manifest, MatrixGates
from packages.pms.replay_service import execute_replay
from packages.pms.walk_forward import phase_cells


def prepare_inputs(snapshot, windows, candidates, symbols=None):
    datasets, gates = {}, {}
    for window in windows:
        datasets[window.name] = {}
        for phase in ("TRAIN", "TEST"):
            cells = phase_cells(window, candidates, phase)
            manifest = input_manifest(snapshot, cells, symbols)
            datasets[window.name][phase] = manifest
            gates[window.name, phase] = MatrixGates(snapshot, manifest)
    return datasets, gates


def replay_executor(snapshot, datasets, report_path):
    report_path = Path(report_path)

    def execute(window, phase, cell, cfg):
        symbols = [r["instrument_id"] for r in datasets[window.name][phase]["universe"]]
        result, diagnostics = execute_replay(
            snapshot, symbols, cfg, cell.case, cell.period.start, cell.period.end
        )
        # Matrix checkpoint files are in this folder; journal paths are relative to it.
        folder = report_path.parent / (report_path.stem + "_folds") / "journals"
        folder.mkdir(parents=True, exist_ok=True)
        destination = folder / (cell.key + ".json")
        payload = json.dumps(
            plain(asdict(result)), sort_keys=True, allow_nan=False
        ).encode()
        temporary = destination.with_suffix(".json.tmp")
        temporary.write_bytes(payload)
        temporary.replace(destination)
        diagnostics.update(
            evidence_file="journals/" + destination.name,
            evidence_sha256=hashlib.sha256(payload).hexdigest(),
        )
        return result, diagnostics

    return execute


def persist_walk_forward(engine, report):
    # Existing research_runs is sufficient; no migration, orders, portfolio or config writes.
    compact = deepcopy(report)
    compact.pop("config", None)
    compact.pop("datasets", None)
    params = dict(
        id=report["run_id"],
        s=min(w["window"]["train_start"] for w in report["folds"]),
        e=max(w["window"]["test_end"] for w in report["folds"]),
        version=report["config"]["version"],
        cfg=json.dumps(
            plain(
                {
                    k: report[k]
                    for k in ("identity", "config", "datasets", "code_version")
                }
            ),
            allow_nan=False,
        ),
        metrics=json.dumps(plain(compact), allow_nan=False),
    )
    with engine.begin() as conn:
        changed = conn.execute(
            text(
                """insert into pms.research_runs(id,run_type,variant,start_date,end_date,config,metrics,strategy_version)
        values(:id,'WALK_FORWARD_V2','FROZEN_TRAIN_ONLY',:s,:e,cast(:cfg as jsonb),cast(:metrics as jsonb),:version)
        on conflict(id) do update set metrics=excluded.metrics
        where research_runs.run_type='WALK_FORWARD_V2' and research_runs.config->>'identity'=cast(:cfg as jsonb)->>'identity'"""
            ),
            params,
        )
        if changed.rowcount != 1:
            raise ValueError("Walk-forward identity conflict")
