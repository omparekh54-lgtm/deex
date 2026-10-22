from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class CostBreakdown:
    turnover: float
    brokerage: float
    exchange_charge: float
    sebi_fee: float
    stt: float
    stamp_duty: float
    gst: float
    slippage: float
    impact: float
    total: float


def _pct(value: float, pct: float) -> float:
    return value * pct / 100.0


def estimate_delivery_costs(
    *,
    trade_date: date,
    side: str,
    price: float,
    quantity: int,
    cfg: dict,
) -> CostBreakdown:
    if quantity <= 0 or price <= 0:
        raise ValueError("price and quantity must be positive")
    side = side.upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError("side must be BUY or SELL")

    turnover = price * quantity
    brokerage = max(
        turnover * float(cfg.get("brokerage_bps", 0.0)) / 10_000.0,
        float(cfg.get("brokerage_flat_per_order", 0.0)),
    )
    exchange_pct = (
        float(cfg["nse_cash_transaction_charge_pct_from_2026_03_01"])
        if trade_date >= date(2026, 3, 1)
        else float(cfg["nse_cash_transaction_charge_pct_before_2026_03_01"])
    )
    exchange_charge = _pct(turnover, exchange_pct)
    sebi_fee = _pct(turnover, float(cfg["sebi_turnover_pct"]))
    stt = _pct(turnover, float(cfg["delivery_stt_pct"]))
    stamp_duty = _pct(turnover, float(cfg["delivery_stamp_duty_pct"])) if side == "BUY" else 0.0
    gst_base = brokerage + exchange_charge + sebi_fee
    gst = _pct(gst_base, float(cfg["gst_pct"]))
    slippage = turnover * float(cfg.get("slippage_bps", 0.0)) / 10_000.0
    impact = turnover * float(cfg.get("impact_bps", 0.0)) / 10_000.0
    total = brokerage + exchange_charge + sebi_fee + stt + stamp_duty + gst + slippage + impact
    return CostBreakdown(
        turnover=round(turnover, 2),
        brokerage=round(brokerage, 2),
        exchange_charge=round(exchange_charge, 2),
        sebi_fee=round(sebi_fee, 2),
        stt=round(stt, 2),
        stamp_duty=round(stamp_duty, 2),
        gst=round(gst, 2),
        slippage=round(slippage, 2),
        impact=round(impact, 2),
        total=round(total, 2),
    )
