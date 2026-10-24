"""Strategy library: ten price-only NSE strategies selectable alongside the PMS Setup 1 replay.

This package is independent of the PMS A/B/C/E modules. It does not import, modify or
replace anything in ``packages.pms`` or the original ``/api/runs`` workflow.
"""
from packages.strategies.catalog import CATALOG, STRATEGY_IDS, public_catalog  # noqa: F401
