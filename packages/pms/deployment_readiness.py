"""Read-only deployment inventory. Never promotes execution or certifies a campaign."""

from __future__ import annotations

import hashlib
import math
from pathlib import Path
from urllib.parse import urlsplit

from sqlalchemy import text

from packages.pms.data_integrity import SCHEMA_SQL, schema_findings
from packages.pms.connection_config import placeholder_password, tls_connect_options

SAFETY_SQL = """select execution_mode,new_orders_enabled,emergency_stop,locked_reason
from pms.safety_state where singleton=true"""
DQ_SQL = """select count(*) from pms.data_quality_issues
where severity='ERROR' and resolved_at is null"""

COMPONENTS = [
    {
        "name": "frontend",
        "entrypoint": "apps/web",
        "command": "npm run build; npm start -- --hostname 0.0.0.0 --port $PORT",
        "runtime": "Node.js 22 (CI baseline)",
        "storage": "No durable application data on local disk",
    },
    {
        "name": "api",
        "entrypoint": "apps.api.main_exec:app",
        "command": "uvicorn apps.api.main_exec:app --host 0.0.0.0 --port $PORT",
        "runtime": "Python 3.12 (CI baseline)",
        "storage": "PostgreSQL pms schema",
    },
    {
        "name": "daily_runner",
        "entrypoint": "scripts/run_market_close.py",
        "command": "python scripts/run_market_close.py",
        "runtime": "Python 3.12; after ingestion; exchange-calendar guarded",
        "storage": "PostgreSQL journals plus external report storage",
    },
    {
        "name": "data_collector",
        "entrypoint": "scripts/run_cloud_collection.py",
        "command": "python scripts/run_cloud_collection.py --run",
        "runtime": "Official bulk sources plus bounded incremental XML processing; full-universe deployment verification required",
        "storage": "Versioned source evidence and PostgreSQL",
    },
]

FOLLOW_UP = [
    (
        "CLOUD_DATA",
        "Phase 2",
        "Prove full cloud ingestion and source freshness without a laptop",
    ),
    (
        "CLOUD_SERVICES",
        "Phase 3",
        "Deploy services, secrets and persistent storage; measure free-tier resource limits",
    ),
    (
        "HISTORICAL_EVIDENCE",
        "Phase 4",
        "Execute matrix, walk-forward and full integrity/source reviews",
    ),
    (
        "RECOVERY_ALERTS",
        "Phase 5",
        "Demonstrate backups/isolated restore, missed-run detection and alerts",
    ),
    (
        "UNATTENDED_REHEARSAL",
        "Phase 6",
        "Demonstrate several actual sessions before starting the campaign clock",
    ),
]

INPUT_PATHS = (
    "requirements.txt",
    "requirements-ops.txt",
    "apps/web/package.json",
    "apps/web/next.config.ts",
    "config/strategy_v1.yaml",
    ".github/workflows/pms-market-close.yml",
)


def check(code, status, detail):
    return dict(code=code, status=status, detail=detail)


def file_inventory(root: Path):
    paths = sorted([*(root / "migrations").glob("*.sql"),
                    *(root / "docs" / "deployment_sql").glob("*.sql")])
    return [
        dict(
            path=str(p.relative_to(root)),
            sha256=hashlib.sha256(p.read_bytes()).hexdigest(),
        )
        for p in paths
    ]


def policy_checks(cfg):
    f = cfg.get("fundamental", {})
    default = f.get("leverage_limits", {}).get("default", {})
    valid = all(
        isinstance(default.get(k), (int, float))
        and not isinstance(default.get(k), bool)
        and math.isfinite(default[k])
        and default[k] >= 0
        for k in ("max_debt_to_equity", "min_interest_coverage")
    )
    return [
        check(
            "LEVERAGE_POLICY",
            "PASS" if valid else "BLOCKED",
            "Explicit finite nonnegative limits configured; provisional values require backtesting before policy validation",
        ),
        check(
            "LEGACY_SURVEILLANCE",
            "REVIEW_REQUIRED",
            "Configured verification flag is not independent archive evidence; reconcile ASM/GSM history",
        ),
        check(
            "PRICE_ADJUSTMENTS",
            "REVIEW_REQUIRED",
            "Verify each deployed feed's adjustment convention against corporate actions",
        ),
        check(
            "VALUATION_POLICY",
            "REVIEW_REQUIRED",
            "Review valuation policy and its intended flag/penalty behavior before freezing the strategy",
        ),
    ]


def environment_checks(env):
    from sqlalchemy.engine import make_url

    result = []
    raw = env.get("DATABASE_URL") or env.get("PMS_DATABASE_URL")
    database_ok = False
    if raw:
        try:
            u = make_url(raw)
            database_ok = (
                u.get_backend_name() == "postgresql"
                and bool(u.host and u.database and u.username)
                and u.host.lower() not in {"localhost", "127.0.0.1", "::1"}
                and tls_connect_options(u.query, env, host=u.host or "")["sslmode"] == "verify-full"
                and not placeholder_password(u.password)
                and not any(
                    x in raw.upper()
                    for x in (
                        "YOUR_DATABASE_PASSWORD",
                        "PROJECT_REF",
                        "YOUR_POOLER_HOST",
                    )
                )
            )
        except Exception:
            pass
    result.append(
        check(
            "DATABASE_CONFIGURATION",
            "PASS" if database_ok else "BLOCKED",
            "Cloud PostgreSQL must use verify-full via URL or deployment policy, with trusted CA; connection/password validity checked separately",
        )
    )
    key = env.get("OPS_API_KEY", "")
    result.append(
        check(
            "OPERATOR_KEY",
            "PASS" if 32 <= len(key) <= 256 and not key.isspace() else "BLOCKED",
            "Private 32..256 character key required; length alone does not prove randomness",
        )
    )
    try:
        base = urlsplit(env.get("PMS_API_BASE_URL", ""))
        api_ok = (
            base.scheme == "https"
            and bool(base.hostname)
            and not base.username
            and not base.password
            and not base.query
            and not base.fragment
            and base.hostname not in {"localhost", "127.0.0.1", "::1"}
        )
    except ValueError:
        api_ok = False
    result.append(
        check(
            "FRONTEND_API_ORIGIN",
            "PASS" if api_ok else "BLOCKED",
            "Private server-side PMS_API_BASE_URL must identify the deployed HTTPS API",
        )
    )
    paper = env.get("EXECUTION_MODE") == "PAPER" and env.get(
        "LIVE_TRADING_ENABLED", ""
    ).lower() in {"false", "0"}
    result.append(
        check(
            "PAPER_CONFIGURATION",
            "PASS" if paper else "BLOCKED",
            "Explicit PAPER mode and LIVE_TRADING_ENABLED=false required; database safety checked separately",
        )
    )
    startup = env.get("RUN_REFERENCE_GAP_BACKFILL", "0").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    result.append(
        check(
            "STARTUP_BACKFILL",
            "BLOCKED" if startup else "PASS",
            "Run ingestion as explicit scheduled jobs, not web startup",
        )
    )
    result.append(
        check(
            "PRIVATE_HTTP_EXCEPTION",
            "BLOCKED" if env.get("PMS_ALLOW_PRIVATE_HTTP", "0") == "1" else "PASS",
            "Cloud operator transport must use HTTPS",
        )
    )
    result.append(check('CLOUD_ENTRY_GATE','PASS' if env.get('PMS_CLOUD_COLLECTION_REQUIRED')=='1' else 'BLOCKED',
                        'Set PMS_CLOUD_COLLECTION_REQUIRED=1 on every cloud API/worker/daily runner; missing or partial source journals block new PAPER entries'))
    return result


def database_checks(conn):
    """Caller must establish READ ONLY transaction before invoking this function."""
    missing = schema_findings(conn.execute(text(SCHEMA_SQL)).mappings().all())
    result = [
        check(
            "DATABASE_SCHEMA",
            "BLOCKED" if missing else "PASS",
            {
                "missing_contract_columns": missing,
                "migration_history": "NOT_INFERRED_FROM_SCHEMA",
            },
        )
    ]
    if missing:
        return result
    state = conn.execute(text(SAFETY_SQL)).mappings().first()
    if state is None:
        result.append(
            check("DATABASE_SAFETY", "BLOCKED", "Safety singleton unavailable")
        )
    else:
        # Preserve safety locks; report only whether one exists, never arbitrary stored text.
        restricted = (
            state["execution_mode"] == "PAPER" and not state["new_orders_enabled"]
        )
        result.append(
            check(
                "DATABASE_SAFETY",
                "PASS" if restricted else "BLOCKED",
                dict(
                    paper_mode=state["execution_mode"] == "PAPER",
                    new_orders_enabled=bool(state["new_orders_enabled"]),
                    emergency_stop=bool(state["emergency_stop"]),
                    lock_present=bool(state["locked_reason"]),
                    note="Restrictive baseline only; locks/stops must be investigated before campaign admission",
                ),
            )
        )
        if state["locked_reason"] or state["emergency_stop"]:
            result.append(
                check(
                    "CAMPAIGN_SAFETY",
                    "BLOCKED",
                    "Existing stop/lock remains active; no automatic clearance",
                )
            )
    errors = int(conn.execute(text(DQ_SQL)).scalar_one())
    result.append(
        check(
            "CRITICAL_DQ",
            "BLOCKED" if errors else "PASS",
            {
                "open_errors": errors,
                "note": "Zero open issues is not full data certification",
            },
        )
    )
    return result


def assess(root, cfg, env, *, plan=False, db_checks=None):
    root = Path(root)
    migrations = file_inventory(root)
    inputs = [
        dict(
            path=p,
            sha256=(
                hashlib.sha256((root / p).read_bytes()).hexdigest()
                if (root / p).is_file()
                else None
            ),
        )
        for p in INPUT_PATHS
    ]
    checks = [
        check(
            "MIGRATION_INVENTORY",
            "PASS" if migrations else "BLOCKED",
            {
                "files": len(migrations),
                "applied": "NOT_VERIFIED",
                "action": "Inspect populated database and back up before selecting changes; never replay all files blindly",
            },
        )
    ]
    checks.append(
        check(
            "DEPLOYMENT_INPUTS",
            "PASS" if all(i["sha256"] for i in inputs) else "BLOCKED",
            "Hashed runtime, strategy and scheduling inputs; contents and secrets not embedded",
        )
    )
    checks += policy_checks(cfg)
    checks += (
        [
            check(
                "RUNTIME_ENVIRONMENT",
                "NOT_CHECKED",
                "Plan mode does not inspect environment or connect",
            )
        ]
        if plan
        else environment_checks(env)
    )
    checks += (
        db_checks
        if db_checks is not None
        else [
            check(
                "DATABASE_INSPECTION",
                "NOT_CHECKED",
                "Use explicit --check --database for read-only inspection",
            )
        ]
    )
    checks += [
        check(code, "NOT_VERIFIED", {"owner_phase": phase, "requirement": requirement})
        for code, phase, requirement in FOLLOW_UP
    ]
    checks += [
        check(
            "DEPENDENCY_REPRODUCIBILITY",
            "REVIEW_REQUIRED",
            "requirements.txt includes unpinned nse and Finstack @main; frontend has no committed lockfile. Freeze verified dependencies before deployment",
        ),
        check(
            "SCHEDULER_COVERAGE",
            "REVIEW_REQUIRED",
            "Cloud workflow implements the full collection chain with 19:07/19:27/19:47 IST attempts and separately enabled PAPER. Activation, secret availability, schedule delivery, initial backlog completion and missed-run monitoring remain unverified. Select one scheduler owner",
        ),
    ]
    return dict(
        version="DEPLOYMENT_READINESS_v1",
        status="PLAN" if plan else "BLOCKED",
        phase_1="ASSESSMENT_IMPLEMENTED",
        campaign_ready=False,
        database_writes=False,
        components=COMPONENTS,
        migrations=migrations,
        inputs=inputs,
        config_sha256=hashlib.sha256(json_config(cfg)).hexdigest(),
        checks=checks,
        runtime_budget={
            "status": "MEASURE_REQUIRED",
            "existing_daily_job_timeout_minutes": 45,
            "schedulers_per_session": 3,
            "measure": [
                "peak RAM",
                "ingest/run duration",
                "CPU",
                "database connections/storage",
                "artifact retention",
                "provider quotas and scheduled-run delivery",
            ],
            "cost_policy": "FREE_FIRST_NO_PAID_PROVISIONING_AUTHORIZED",
        },
        next_phase="Phase 2: reliable cloud data collection",
        notes=[
            "No cloud resources provisioned, migrations applied, orders enabled or campaign started",
            "Deployment assessment cannot certify historical results, full data integrity or 2–3 months of forward evidence",
        ],
    )


def json_config(cfg):
    import json

    return json.dumps(
        cfg, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
