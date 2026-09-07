"""
replay_and_synthesize.py
-------------------------
Phase 1b: Build the hybrid real+synthetic dataset.

- Loads REAL orders, order_items, and payments from the Olist dataset
- Takes a slice: the most recent N_DAYS (by order_purchase_timestamp) of that
  real historical data
- TIME-SHIFTS every timestamp in that slice so the most recent real order
  maps to "now", preserving relative gaps between orders (so seasonality /
  patterns in the real data survive the shift)
- For every real order, SYNTHESIZES a plausible browsing session using Faker:
  page_view(s) -> add_to_cart(s) -> checkout -> purchase, ending at the
  real order's (shifted) purchase timestamp. Includes 1-2 "browsed but not
  bought" products per session so Conversion Rate is meaningful downstream.

This script does NOT touch Kafka yet (that's Phase 2) - it writes local
JSONL files to data/synthesized/ so we can inspect the output first.

Run with: uv run python src/generator/replay_and_synthesize.py
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

DATA_DIR = Path("data/raw_olist")
OUT_DIR = Path("data/synthesized")
N_DAYS = 60  # how many days of real (historical) order data to replay

fake = Faker()


def load_slice():
    """Load real orders/items/payments, filtered to the most recent N_DAYS
    of order_purchase_timestamp found in the dataset."""
    orders = pd.read_csv(
        DATA_DIR / "olist_orders_dataset.csv",
        parse_dates=["order_purchase_timestamp"],
    )
    items = pd.read_csv(DATA_DIR / "olist_order_items_dataset.csv")
    payments = pd.read_csv(DATA_DIR / "olist_order_payments_dataset.csv")
    products = pd.read_csv(DATA_DIR / "olist_products_dataset.csv")

    max_ts = orders["order_purchase_timestamp"].max()
    cutoff = max_ts - timedelta(days=N_DAYS)
    orders_slice = orders[orders["order_purchase_timestamp"] >= cutoff].copy()

    order_ids = set(orders_slice["order_id"])
    items_slice = items[items["order_id"].isin(order_ids)].copy()
    payments_slice = payments[payments["order_id"].isin(order_ids)].copy()

    return orders_slice, items_slice, payments_slice, products, max_ts


def compute_shift(max_ts: pd.Timestamp) -> timedelta:
    """Shift so the most recent real order timestamp becomes 'now'."""
    now = datetime.now()
    return now - max_ts.to_pydatetime()


def synthesize_session(order_row, item_rows, all_product_ids, shift: timedelta):
    """Build a list of event dicts (page_view -> add_to_cart -> checkout ->
    purchase) leading up to a single real order's (shifted) purchase time."""
    purchase_time = order_row["order_purchase_timestamp"].to_pydatetime() + shift
    session_id = str(uuid.uuid4())
    customer_id = order_row["customer_id"]
    order_id = order_row["order_id"]

    real_product_ids = list(item_rows["product_id"])
    # Add 1-2 "browsed but not bought" products for a realistic funnel
    browsed_only = random.sample(all_product_ids, k=random.randint(1, 2))
    all_session_products = real_product_ids + browsed_only
    random.shuffle(all_session_products)

    events = []
    # Session starts 5-30 minutes before the actual purchase
    session_start_offset = timedelta(minutes=random.randint(5, 30))
    t = purchase_time - session_start_offset

    # page_view for every product looked at in the session
    for pid in all_session_products:
        events.append({
            "event_type": "page_view",
            "event_time": t.isoformat(),
            "session_id": session_id,
            "customer_id": customer_id,
            "product_id": pid,
        })
        t += timedelta(seconds=random.randint(10, 90))

    # add_to_cart only for the products actually purchased
    for pid in real_product_ids:
        events.append({
            "event_type": "add_to_cart",
            "event_time": t.isoformat(),
            "session_id": session_id,
            "customer_id": customer_id,
            "product_id": pid,
        })
        t += timedelta(seconds=random.randint(10, 60))

    # checkout
    events.append({
        "event_type": "checkout",
        "event_time": t.isoformat(),
        "session_id": session_id,
        "customer_id": customer_id,
        "order_id": order_id,
    })

    # purchase - lands exactly on the real (shifted) order timestamp
    events.append({
        "event_type": "purchase",
        "event_time": purchase_time.isoformat(),
        "session_id": session_id,
        "customer_id": customer_id,
        "order_id": order_id,
    })

    return events


def main():
    orders, items, payments, products, max_ts = load_slice()
    shift = compute_shift(max_ts)
    all_product_ids = products["product_id"].tolist()

    print(f"Real order slice: {len(orders):,} orders "
          f"(last {N_DAYS} days of historical data)")
    print(f"Original date range: {orders['order_purchase_timestamp'].min()} "
          f"to {orders['order_purchase_timestamp'].max()}")
    print(f"Time shift applied: {shift}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    events_out = open(OUT_DIR / "events.jsonl", "w")
    orders_out = open(OUT_DIR / "orders.jsonl", "w")
    payments_out = open(OUT_DIR / "payments.jsonl", "w")

    total_events = 0
    for _, order_row in orders.iterrows():
        order_id = order_row["order_id"]
        item_rows = items[items["order_id"] == order_id]
        if item_rows.empty:
            continue  # skip orders with no line items (data gap)

        # Synthesize and write events
        events = synthesize_session(order_row, item_rows, all_product_ids, shift)
        for e in events:
            events_out.write(json.dumps(e) + "\n")
        total_events += len(events)

        # Write the real order record, with ALL lifecycle timestamps shifted
        # by the same amount (purchase, approval, carrier, delivery, estimate)
        # so the sequence of dates stays internally consistent.
        shifted_order = order_row.to_dict()
        date_columns = [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ]
        for col in date_columns:
            raw_val = order_row.get(col)
            if pd.isna(raw_val):
                shifted_order[col] = None
                continue
            parsed = pd.to_datetime(raw_val).to_pydatetime()
            shifted_order[col] = (parsed + shift).isoformat()
        orders_out.write(json.dumps(shifted_order, default=str) + "\n")

        # Write real payment record(s) tied to this order
        pay_rows = payments[payments["order_id"] == order_id]
        for _, pay_row in pay_rows.iterrows():
            payments_out.write(json.dumps(pay_row.to_dict(), default=str) + "\n")

    events_out.close()
    orders_out.close()
    payments_out.close()

    print(f"\n✅ Wrote {len(orders):,} orders, {total_events:,} synthesized "
          f"events, and payments to {OUT_DIR}/")


if __name__ == "__main__":
    main()
