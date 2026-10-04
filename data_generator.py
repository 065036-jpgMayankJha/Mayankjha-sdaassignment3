import random
import uuid
from datetime import datetime, timezone, timedelta

PRODUCTS = [
    ("P1001", "Wireless Earbuds", "Audio", 2499),
    ("P1002", "Smart Watch", "Wearables", 4999),
    ("P1003", "Bluetooth Speaker", "Audio", 3299),
    ("P1004", "Laptop Backpack", "Lifestyle", 1799),
    ("P1005", "Mechanical Keyboard", "Computers", 5499),
    ("P1006", "USB-C Hub", "Computers", 2199),
    ("P1007", "Power Bank", "Mobile Accessories", 1499),
    ("P1008", "Fitness Band", "Wearables", 2799),
]

WAREHOUSES = ["WH_DELHI", "WH_MUMBAI", "WH_BENGALURU", "WH_HYDERABAD"]
COURIERS = ["BlueDart", "Delhivery", "Ecom Express", "DTDC"]
DEVICES = ["mobile", "desktop", "tablet"]
PLATFORMS = ["android", "ios", "web"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart", "search", "checkout_start"]
PAYMENT_MODES = ["UPI", "card", "wallet", "COD"]
ORDER_STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled", "returned"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def make_clickstream():
    product_id, _, category, _ = random.choice(PRODUCTS)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"U{random.randint(1001, 1100)}",
        "session_id": f"S{random.randint(10001, 10150)}",
        "event_type": random.choice(EVENT_TYPES),
        "product_id": product_id,
        "category": category,
        "device_type": random.choice(DEVICES),
        "platform": random.choice(PLATFORMS),
        "timestamp": now_iso(),
    }


def make_order():
    product_id, _, category, price = random.choice(PRODUCTS)
    qty = random.randint(1, 3)
    discount = random.choice([0, 0, 5, 10, 15])
    gross = price * qty
    order_value = round(gross * (1 - discount / 100), 2)
    return {
        "order_id": f"O{uuid.uuid4().hex[:10].upper()}",
        "customer_id": f"C{random.randint(1001, 1100)}",
        "items": [{
            "product_id": product_id,
            "category": category,
            "qty": qty,
            "price": price,
        }],
        "payment_mode": random.choice(PAYMENT_MODES),
        "payment_status": random.choices(
            ["success", "failed", "refunded"], weights=[88, 9, 3], k=1
        )[0],
        "order_status": random.choice(ORDER_STATUSES),
        "order_value": order_value,
        "discount_applied": discount,
        "timestamp": now_iso(),
    }


def make_inventory():
    product_id, _, _, _ = random.choice(PRODUCTS)
    quantity = random.randint(1, 15)
    change_type = random.choice(
        ["inbound", "reserved", "dispatched", "returned_to_stock", "damaged"]
    )
    current_stock = random.randint(10, 250)
    return {
        "inventory_event_id": str(uuid.uuid4()),
        "warehouse_id": random.choice(WAREHOUSES),
        "product_id": product_id,
        "sku": f"SKU-{product_id}",
        "stock_change_type": change_type,
        "quantity": quantity,
        "current_stock_level": current_stock,
        "timestamp": now_iso(),
    }


def make_logistics():
    expected = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 5))
    return {
        "shipment_id": f"SH{uuid.uuid4().hex[:10].upper()}",
        "order_id": f"O{uuid.uuid4().hex[:10].upper()}",
        "courier_partner": random.choice(COURIERS),
        "current_status": random.choice(
            ["picked_up", "in_transit", "out_for_delivery", "delivered", "delayed", "RTO"]
        ),
        "current_location": random.choice(
            ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Pune", "Gurugram"]
        ),
        "expected_delivery_date": expected.date().isoformat(),
        "timestamp": now_iso(),
    }


GENERATORS = {
    "clickstream": make_clickstream,
    "orders": make_order,
    "inventory": make_inventory,
    "logistics": make_logistics,
}
