from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from dataclasses import asdict
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine

from packages.pms.config import load_strategy_config
from packages.pms.matrix_replay import ResearchDataGap
from packages.pms.research import ResearchVariant
from packages.pms.research_lab import ResearchLab


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run PMS research variants or robustness grid")
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--capital", type=float, default=None)
    parser.add_argument("--mode", choices=["variants", "robustness"], default="variants")
    parser.add_argument("--config", type=Path)
    args = parser.parse_args(argv)

    from dotenv import load_dotenv
    from apps.api.main import _render_safe_database_url

    load_dotenv()
    database_url = os.getenv("DATABASE_URL") or os.getenv("PMS_DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL or PMS_DATABASE_URL is required")
    engine = create_engine(_render_safe_database_url(database_url), pool_pre_ping=True,
                           connect_args={"options": "-c search_path=pms,public"})
    try:
        symbols = [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
        cfg = load_strategy_config(args.config) if args.config else load_strategy_config()
        lab = ResearchLab(engine, symbols, date.fromisoformat(args.start), date.fromisoformat(args.end), args.capital, cfg=cfg)

        try:
            if args.mode == "variants":
                result = lab.run_all_variants()
            else:
                result = [asdict(x) for x in lab.run_robustness(ResearchVariant("FULL_A_B_C_E"))]
        except ResearchDataGap as exc:
            print(json.dumps({"status": "BLOCKED", "blockers": str(exc).split(";")}, indent=2))
            return 2
        print(json.dumps(result, default=str, indent=2))
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
