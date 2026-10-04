import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

MYSQL_HOST = os.getenv(
    "MYSQL_HOST",
    "localhost"
)

MYSQL_PORT = int(
    os.getenv(
        "MYSQL_PORT",
        "3307"
    )
)

MYSQL_DATABASE = os.getenv(
    "MYSQL_DATABASE",
    "ecommerce_sda"
)

MYSQL_USER = os.getenv(
    "MYSQL_USER",
    "sda_user"
)

MYSQL_PASSWORD = os.getenv(
    "MYSQL_PASSWORD",
    "sda_password"
)


TOPICS = {
    "clickstream": "topic_clickstream",
    "orders": "topic_orders",
    "inventory": "topic_inventory",
    "logistics": "topic_logistics",
}