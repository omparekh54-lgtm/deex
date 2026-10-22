"""Moving-average cost basis, including entry fees, across repeated trade cycles."""

import math


def ledger_positions(rows):
    positions = {}
    for row in rows:
        pk = int(row["instrument_id"])
        qty = int(row["quantity"])
        price = float(row["price"] or 0)
        fees = float(row.get("fees") or 0)
        if not all(math.isfinite(v) and v >= 0 for v in (price, fees)) or (
            qty and price <= 0
        ):
            raise ValueError("Invalid PAPER ledger fill")
        lot = positions.setdefault(pk, {"quantity": 0, "cost": 0.0})
        if qty > 0:
            lot["quantity"] += qty
            lot["cost"] += qty * price + fees
        elif qty < 0:
            if lot["quantity"] < -qty:
                raise ValueError("PAPER ledger sells exceed holdings")
            lot["cost"] *= (lot["quantity"] + qty) / lot["quantity"]
            lot["quantity"] += qty
    return {
        pk: dict(quantity=v["quantity"], average_price=v["cost"] / v["quantity"])
        for pk, v in positions.items()
        if v["quantity"]
    }
