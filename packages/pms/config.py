from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "strategy_v1.yaml"


@lru_cache(maxsize=8)
def _load_strategy_config_cached(path: str) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_strategy_config(path: str | Path = DEFAULT_CONFIG) -> dict[str, Any]:
    """Return an isolated config copy so research/test overrides cannot leak globally."""
    return deepcopy(_load_strategy_config_cached(str(Path(path))))
