"""Read the local data folder written by ``scripts/fetch_all_data.py``."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd

from packages.strategies.bhavcopy import KEEP_SERIES, adjust


def data_root(root: str | os.PathLike | None = None) -> Path:
    path = Path(root or os.getenv("DEEX_DATA_DIR", "deex_data")).resolve()
    if not (path / "prices.parquet").exists():
        raise FileNotFoundError(
            f"No price table in {path}. Run: python scripts/fetch_all_data.py --root {path}")
    return path


def raw_prices(root: Path, start: date, end: date, series=KEEP_SERIES) -> pd.DataFrame:
    df = pd.read_parquet(root / "prices.parquet",
                         filters=[("date", ">=", pd.Timestamp(start)), ("date", "<=", pd.Timestamp(end))])
    return df[df.series.isin(series)].reset_index(drop=True)


def load_panel(root: Path, start: date, end: date) -> pd.DataFrame:
    """Same adjusted panel the S1-S10 engines build from bhavcopies, from the local table."""
    return adjust(raw_prices(root, start, end))


def coverage(root: Path) -> dict:
    import json
    try:
        return json.loads((root / "manifest.json").read_text())
    except (OSError, ValueError):
        return {}
