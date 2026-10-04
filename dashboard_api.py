from flask import Flask, jsonify
from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB

app = Flask(__name__)

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]


@app.route("/orders")
def orders():
    data = list(
        db["orders"]
        .find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(500)
    )
    return jsonify(data)


@app.route("/clickstream")
def clickstream():
    data = list(
        db["clickstream"]
        .find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(500)
    )
    return jsonify(data)


@app.route("/inventory")
def inventory():
    data = list(
        db["inventory"]
        .find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(500)
    )
    return jsonify(data)


@app.route("/logistics")
def logistics():
    data = list(
        db["logistics"]
        .find({}, {"_id": 0})
        .sort("timestamp", -1)
        .limit(500)
    )
    return jsonify(data)

@app.route("/order_value_over_time")
def order_value_over_time():
    data = list(
        db["orders"].find(
            {},
            {
                "_id": 0,
                "timestamp": 1,
                "order_value": 1
            }
        ).sort("timestamp", 1).limit(500)
    )

    result = []

    for item in data:
        result.append({
            "time": item.get("timestamp"),
            "order_value": float(item.get("order_value", 0))
        })

    return jsonify(result)
@app.route("/orders_count")
def orders_count():
    total = db["orders"].count_documents({})
    return jsonify({"value": total})


@app.route("/total_order_value")
def total_order_value():
    result = db["orders"].aggregate([
        {
            "$group": {
                "_id": None,
                "total": {"$sum": "$order_value"}
            }
        }
    ])

    data = list(result)
    total = data[0]["total"] if data else 0

    return jsonify({"value": float(total)})


@app.route("/avg_order_value")
def avg_order_value():
    result = db["orders"].aggregate([
        {
            "$group": {
                "_id": None,
                "average": {"$avg": "$order_value"}
            }
        }
    ])

    data = list(result)
    average = data[0]["average"] if data else 0

    return jsonify({"value": float(average)})


@app.route("/unique_customers")
def unique_customers():
    customers = db["orders"].distinct("customer_id")

    return jsonify({
        "value": len(customers)
    })
@app.route("/health")
def health():
    return jsonify({"status": "API is running"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)