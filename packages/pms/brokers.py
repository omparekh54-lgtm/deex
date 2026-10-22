from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, Any
from uuid import uuid4
import math
from packages.pms.ledger import ledger_positions

from sqlalchemy import Engine, text


@dataclass(frozen=True)
class BrokerOrderRequest:
    instrument_id: str
    side: str
    quantity: int
    order_type: str = "MARKET"
    limit_price: float | None = None
    trigger_price: float | None = None
    idempotency_key: str = ""


@dataclass(frozen=True)
class BrokerOrderResult:
    order_id: str
    status: str
    external_order_id: str | None = None


class Broker(Protocol):
    name: str

    def place_order(self, request: BrokerOrderRequest) -> BrokerOrderResult: ...
    def cancel_order(self, order_id: str) -> dict[str, Any]: ...
    def get_orders(self) -> list[dict[str, Any]]: ...
    def get_positions(self) -> list[dict[str, Any]]: ...
    def get_cash(self) -> float: ...


class _InstrumentLookup:
    def __init__(self, engine: Engine):
        self.engine = engine

    def row(self, instrument_id: str) -> dict[str, Any]:
        exchange, symbol = instrument_id.split(":", 1)
        trading_symbol = f"{symbol}-EQ" if exchange == "NSE" else symbol
        with self.engine.connect() as conn:
            row = (
                conn.execute(
                    text("""
                    select id, exchange, trading_symbol, nse_symbol
                    from instruments
                    where exchange=:e and trading_symbol=:s
                    order by valid_from desc limit 1
                    """),
                    {"e": exchange, "s": trading_symbol},
                )
                .mappings()
                .first()
            )
        if not row:
            raise KeyError(instrument_id)
        return dict(row)


class PaperBroker:
    """Internal fake-money broker.

    `PAPER` is reserved for forward strategy validation. `PAPER_SANDBOX` is used
    by the interactive app module so manual experiments never contaminate the
    formal paper-validation evidence.
    """

    name = "PAPER"

    def __init__(
        self,
        engine: Engine,
        initial_cash: float = 10_000_000,
        *,
        account_mode: str = "PAPER",
        broker_name: str | None = None,
    ):
        self.engine = engine
        self.lookup = _InstrumentLookup(engine)
        self.initial_cash = float(initial_cash)
        self.account_mode = account_mode.upper()
        if self.account_mode not in {"PAPER", "PAPER_SANDBOX"}:
            raise ValueError("Paper broker requires a paper account")
        if not math.isfinite(self.initial_cash) or self.initial_cash <= 0:
            raise ValueError("Paper capital must be finite and positive")
        self.broker_name = (broker_name or self.account_mode).upper()
        self.name = self.broker_name

    def place_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        if type(request.quantity) is not int or request.quantity <= 0:
            raise ValueError("quantity must be positive")
        side = request.side.upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side must be BUY or SELL")
        kind = request.order_type.upper()
        if kind not in {"MARKET", "LIMIT", "STOP", "STOP_MARKET", "SL-M"}:
            raise ValueError("unsupported paper order type")
        if kind == "LIMIT" and request.limit_price is None:
            raise ValueError("limit_price is required for LIMIT orders")
        if kind in {"STOP", "STOP_MARKET", "SL-M"} and request.trigger_price is None:
            raise ValueError("trigger_price is required for stop orders")

        if any(
            v is not None and (not math.isfinite(float(v)) or v <= 0)
            for v in (request.limit_price, request.trigger_price)
        ):
            raise ValueError("Order prices must be finite and positive")
        instrument = self.lookup.row(request.instrument_id)
        order_id = str(uuid4())
        status = "OPEN"
        with self.engine.begin() as conn:
            conn.execute(
                text("select pg_advisory_xact_lock(hashtext(:key))"),
                {
                    "key": (
                        "pms_formal_paper_account"
                        if self.account_mode == "PAPER"
                        else "pms_account:" + self.account_mode
                    )
                },
            )
            existing = (
                conn.execute(
                    text("select * from broker_orders where idempotency_key=:k"),
                    {"k": request.idempotency_key},
                )
                .mappings()
                .first()
                if request.idempotency_key
                else None
            )
            if existing:
                if (
                    existing["mode"],
                    existing["broker"],
                    int(existing["instrument_id"]),
                    existing["side"],
                    int(existing["quantity"]),
                    existing["order_type"],
                    (
                        float(existing["limit_price"])
                        if existing["limit_price"] is not None
                        else None
                    ),
                    (
                        float(existing["trigger_price"])
                        if existing["trigger_price"] is not None
                        else None
                    ),
                ) != (
                    self.account_mode,
                    self.broker_name,
                    int(instrument["id"]),
                    side,
                    request.quantity,
                    kind,
                    (
                        float(request.limit_price)
                        if request.limit_price is not None
                        else None
                    ),
                    (
                        float(request.trigger_price)
                        if request.trigger_price is not None
                        else None
                    ),
                ):
                    raise ValueError(
                        "Idempotency key belongs to a different order or account"
                    )
                return BrokerOrderResult(str(existing["id"]), str(existing["status"]))
            conn.execute(
                text("""
                    insert into broker_orders(
                      id,broker,mode,instrument_id,side,order_type,quantity,
                      limit_price,trigger_price,status,idempotency_key
                    ) values (
                      :id,:broker,:mode,:instrument,:side,:type,:qty,
                      :limit,:trigger,:status,:key
                    )
                    """),
                {
                    "id": order_id,
                    "broker": self.broker_name,
                    "mode": self.account_mode,
                    "instrument": instrument["id"],
                    "side": side,
                    "type": kind,
                    "qty": request.quantity,
                    "limit": request.limit_price,
                    "trigger": request.trigger_price,
                    "status": status,
                    "key": request.idempotency_key or order_id,
                },
            )
        return BrokerOrderResult(order_id, status)

    def fill_order(
        self,
        order_id: str,
        price: float,
        quantity: int | None = None,
        fees: float = 0.0,
    ) -> dict[str, Any]:
        if (
            not math.isfinite(price)
            or price <= 0
            or not math.isfinite(fees)
            or fees < 0
        ):
            raise ValueError("fill price/fees must be finite and valid")
        with self.engine.begin() as conn:
            conn.execute(
                text("select pg_advisory_xact_lock(hashtext(:key))"),
                {
                    "key": (
                        "pms_formal_paper_account"
                        if self.account_mode == "PAPER"
                        else "pms_account:" + self.account_mode
                    )
                },
            )
            order = (
                conn.execute(
                    text(
                        "select * from broker_orders where id=:id and broker=:broker and mode=:mode for update"
                    ),
                    {
                        "id": order_id,
                        "broker": self.broker_name,
                        "mode": self.account_mode,
                    },
                )
                .mappings()
                .one()
            )
            if (order.get("raw_response") or {}).get("formal_paper"):
                raise PermissionError(
                    "Formal strategy fills require the transactional lifecycle executor"
                )
            if str(order["status"]).upper() not in {"OPEN", "PARTIALLY_FILLED"}:
                raise ValueError("only OPEN paper orders can be filled")
            already = int(
                conn.execute(
                    text(
                        "select coalesce(sum(quantity),0) from broker_fills where order_id=:id"
                    ),
                    {"id": order_id},
                ).scalar_one()
            )
            remaining = int(order["quantity"]) - already
            qty = remaining if quantity is None else quantity
            if (
                not isinstance(qty, int)
                or isinstance(qty, bool)
                or qty <= 0
                or qty > remaining
            ):
                raise ValueError("invalid fill quantity")

            side = str(order["side"]).upper()
            if side == "BUY":
                required_cash = float(price) * qty + float(fees)
                if required_cash > self.get_cash():
                    raise ValueError("insufficient paper cash")
            else:
                held = conn.execute(
                    text("""
                        select coalesce(sum(quantity),0)
                        from portfolio_ledger
                        where account_mode=:mode and event_type='FILL' and instrument_id=:instrument
                        """),
                    {"mode": self.account_mode, "instrument": order["instrument_id"]},
                ).scalar_one()
                if int(held or 0) < qty:
                    raise ValueError("insufficient paper position for SELL")

            realized = 0.0
            if side == "SELL":
                fills = (
                    conn.execute(
                        text(
                            """select instrument_id,quantity,price,fees from portfolio_ledger
                    where account_mode=:mode and event_type='FILL' and instrument_id=:instrument order by event_time,id"""
                        ),
                        {
                            "mode": self.account_mode,
                            "instrument": order["instrument_id"],
                        },
                    )
                    .mappings()
                    .all()
                )
                avg_cost = ledger_positions(fills)[int(order["instrument_id"])][
                    "average_price"
                ]
                realized = (float(price) - avg_cost) * qty - float(fees)

            fill_id = str(uuid4())
            now = datetime.now(timezone.utc)
            conn.execute(
                text("""
                    insert into broker_fills(id,order_id,external_trade_id,fill_time,price,quantity,fees)
                    values (:id,:oid,:tid,:t,:p,:q,:f)
                    """),
                {
                    "id": fill_id,
                    "oid": order_id,
                    "tid": fill_id,
                    "t": now,
                    "p": price,
                    "q": qty,
                    "f": fees,
                },
            )
            conn.execute(
                text(
                    "update broker_orders set status=:status,updated_at=:t where id=:id"
                ),
                {
                    "t": now,
                    "id": order_id,
                    "status": "FILLED" if qty == remaining else "PARTIALLY_FILLED",
                },
            )
            cash_delta = (
                -(price * qty + fees) if side == "BUY" else (price * qty - fees)
            )
            conn.execute(
                text("""
                    insert into portfolio_ledger(
                      account_mode,event_type,instrument_id,order_id,quantity,price,
                      cash_delta,realized_pnl_delta,fees,metadata
                    ) values (
                      :mode,'FILL',:instrument,:oid,:qty,:price,
                      :cash,:realized,:fees,jsonb_build_object('side',:side)
                    )
                    """),
                {
                    "mode": self.account_mode,
                    "instrument": order["instrument_id"],
                    "oid": order_id,
                    "qty": qty if side == "BUY" else -qty,
                    "price": price,
                    "cash": cash_delta,
                    "realized": realized,
                    "fees": fees,
                    "side": side,
                },
            )
        return {
            "fill_id": fill_id,
            "price": price,
            "quantity": qty,
            "realized_pnl": round(realized, 2),
        }

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        with self.engine.begin() as conn:
            conn.execute(
                text("select pg_advisory_xact_lock(hashtext(:key))"),
                {
                    "key": (
                        "pms_formal_paper_account"
                        if self.account_mode == "PAPER"
                        else "pms_account:" + self.account_mode
                    )
                },
            )
            result = conn.execute(
                text("""
                    update broker_orders set status='CANCELLED',updated_at=now()
                    where id=:id and broker=:broker and mode=:mode and status in ('OPEN','PARTIALLY_FILLED')
                    """),
                {"id": order_id, "broker": self.broker_name, "mode": self.account_mode},
            )
        return {
            "order_id": order_id,
            "status": "CANCELLED",
            "changed": bool(result.rowcount),
        }

    def get_orders(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text(
                        "select * from broker_orders where broker=:broker and mode=:mode order by created_at desc"
                    ),
                    {"broker": self.broker_name, "mode": self.account_mode},
                )
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    def get_positions(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = (
                conn.execute(
                    text(
                        """select l.instrument_id,l.quantity,l.price,l.fees,i.exchange,
                coalesce(i.nse_symbol,replace(i.trading_symbol,'-EQ','')) symbol
                from portfolio_ledger l join instruments i on i.id=l.instrument_id
                where l.account_mode=:mode and l.event_type='FILL' order by l.event_time,l.id"""
                    ),
                    {"mode": self.account_mode},
                )
                .mappings()
                .all()
            )
        names = {int(r["instrument_id"]): (r["exchange"], r["symbol"]) for r in rows}
        return [
            {
                "exchange": names[pk][0],
                "symbol": names[pk][1],
                "quantity": v["quantity"],
                "avg_price": v["average_price"],
            }
            for pk, v in sorted(ledger_positions(rows).items())
        ]

    def get_cash(self) -> float:
        with self.engine.connect() as conn:
            delta = conn.execute(
                text(
                    "select coalesce(sum(cash_delta),0) from portfolio_ledger where account_mode=:mode"
                ),
                {"mode": self.account_mode},
            ).scalar_one()
        return float(self.initial_cash) + float(delta)


class DisabledLiveBroker:
    """Compatibility stub: real-money execution is intentionally unavailable."""

    name = "LIVE_DISABLED"

    def __init__(self, engine: Engine, *args, **kwargs):
        self.engine = engine

    @staticmethod
    def _blocked():
        raise PermissionError(
            "Real-money broker execution is disabled. Use the internal PAPER broker."
        )

    def place_order(self, request: BrokerOrderRequest) -> BrokerOrderResult:
        self._blocked()

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        self._blocked()

    def get_orders(self) -> list[dict[str, Any]]:
        return []

    def get_positions(self) -> list[dict[str, Any]]:
        return []

    def get_cash(self) -> float:
        return 0.0


# Keep the old import name so legacy modules fail closed rather than importing a
# broker SDK. Nothing in the PMS can submit a real Angel One order anymore.
AngelOneBroker = DisabledLiveBroker
