from __future__ import annotations

from datetime import date, datetime, time, timedelta

from celery import shared_task
from billiard.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from packages.pms.module_a_pipeline import ModuleAPipeline
from packages.pms.paper_validation import PaperValidationService
from packages.pms.portfolio_accounting import PortfolioAccountingService
from packages.pms.production_readiness import ProductionReadinessController
from packages.pms.safety import SafetyController
from packages.pms.module_a_context import IST
from packages.pms.workers import _engine, _record, _finish, _fail


def _day_end(d: date) -> datetime:
    from packages.pms.module_a_context import end_of_day, IST

    return min(end_of_day(d), datetime.now(IST))


@shared_task(name="pms.financial_refresh")
def financial_refresh(symbols: list[str], trade_date: str | None = None):
    engine, run_id = _record("financial_worker", "module_a_refresh")
    try:
        d = date.fromisoformat(trade_date) if trade_date else datetime.now(IST).date()
        pipeline = ModuleAPipeline(engine)
        results = []
        for instrument_id in symbols:
            try:
                snapshot, decision = pipeline.evaluate(
                    instrument_id, _day_end(d), persist=True
                )
                results.append(
                    {
                        "instrument_id": instrument_id,
                        "eligible": decision.eligible,
                        "score": decision.score,
                    }
                )
            except SoftTimeLimitExceeded:
                raise
            except Exception as exc:
                results.append(
                    {"instrument_id": instrument_id, "error_type": type(exc).__name__}
                )
        _finish(
            engine,
            run_id,
            "PARTIAL" if any("error_type" in r for r in results) else "SUCCESS",
            {"trade_date": str(d), "results": results},
        )
        return results
    except Exception as exc:
        _fail(engine, run_id, exc)
        raise RuntimeError(
            "Financial refresh failed (" + type(exc).__name__ + ")"
        ) from None
    finally:
        engine.dispose()


@shared_task(name="pms.surveillance_guard")
def surveillance_guard(max_age_days: int = 7):
    engine, run_id = _record("surveillance_worker", "surveillance_guard")
    try:
        with engine.connect() as conn:
            latest = conn.execute(
                text("select max(effective_from) from surveillance_history")
            ).scalar_one_or_none()
        stale = (
            latest is None
            or (datetime.now(IST).date() - latest.date()).days > max_age_days
        )
        if stale:
            SafetyController(engine).lock("surveillance_data_stale")
        payload = {
            "latest_surveillance": str(latest) if latest else None,
            "stale": stale,
        }
        _finish(engine, run_id, "DEGRADED" if stale else "SUCCESS", payload)
        return payload
    except Exception as exc:
        _fail(engine, run_id, exc, "surveillance_guard_exception")
        raise RuntimeError(
            "Surveillance guard failed (" + type(exc).__name__ + ")"
        ) from None
    finally:
        engine.dispose()


@shared_task(name="pms.research_snapshot")
def research_snapshot(trade_date: str | None = None):
    engine, run_id = _record("research_worker", "daily_snapshot")
    try:
        d = date.fromisoformat(trade_date) if trade_date else datetime.now(IST).date()
        with engine.connect() as conn:
            selected = int(
                conn.execute(
                    text(
                        "select count(*) from module_c_rankings where trade_date=:d and selected=true"
                    ),
                    {"d": d},
                ).scalar_one()
            )
            regime = (
                conn.execute(
                    text(
                        "select band,max_open_trades from market_regime_history where trade_date=:d"
                    ),
                    {"d": d},
                )
                .mappings()
                .first()
            )
        snap = PortfolioAccountingService(engine).snapshot(d, "PAPER")
        PortfolioAccountingService(engine).persist_snapshot(snap)
        payload = {
            "trade_date": str(d),
            "module_c_selected": selected,
            "regime": dict(regime) if regime else None,
            "paper_nav": snap.nav,
        }
        _finish(engine, run_id, "SUCCESS", payload)
        return payload
    except Exception as exc:
        _fail(engine, run_id, exc)
        raise RuntimeError("Worker failed (" + type(exc).__name__ + ")") from None
    finally:
        engine.dispose()


@shared_task(name="pms.paper_validation")
def paper_validation(lookback_days: int = 60):
    engine, run_id = _record("research_worker", "paper_validation")
    try:
        end = datetime.now(IST).date()
        start = end - timedelta(days=max(1, lookback_days) - 1)
        result = PaperValidationService(engine).evaluate(
            start, end, actor="paper_validation_worker"
        )
        payload = {
            "validation_id": result.run_id,
            "passed": result.passed,
            "metrics": result.metrics,
            "gates": result.gates,
        }
        _finish(engine, run_id, "SUCCESS" if result.passed else "NOT_READY", payload)
        return payload
    except Exception as exc:
        _fail(engine, run_id, exc)
        raise RuntimeError("Worker failed (" + type(exc).__name__ + ")") from None
    finally:
        engine.dispose()


@shared_task(name="pms.readiness_refresh")
def readiness_refresh():
    engine, run_id = _record("execution_worker", "readiness_refresh")
    try:
        controller = ProductionReadinessController(engine)
        result = controller.evaluate()
        controller.persist(result, actor="readiness_worker")
        payload = {
            "stage": result.stage,
            "readiness_status": result.readiness_status,
            "gates": result.gates,
            "details": result.details,
        }
        _finish(engine, run_id, "SUCCESS", payload)
        return payload
    except Exception as exc:
        _fail(engine, run_id, exc)
        raise RuntimeError("Worker failed (" + type(exc).__name__ + ")") from None
    finally:
        engine.dispose()
