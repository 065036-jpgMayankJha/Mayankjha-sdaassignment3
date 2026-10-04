import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

import mysql.connector
from mysql.connector import Error

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
    MYSQL_USER,
    MYSQL_PASSWORD,
    TOPICS,
)


def wait_for_kafka():
    while True:
        try:
            test = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version=(2, 6),
                consumer_timeout_ms=1000,
            )

            test.close()

            print("Kafka is available.")
            return

        except NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(3)


def connect_mysql():
    while True:
        try:
            connection = mysql.connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                database=MYSQL_DATABASE,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
            )

            if connection.is_connected():
                print(
                    f"Connected to MySQL database: {MYSQL_DATABASE}"
                )

                return connection

        except Error as exc:
            print(f"Waiting for MySQL... {exc}")
            time.sleep(3)


def insert_clickstream(cursor, event):

    query = """
        INSERT INTO clickstream (
            event_id,
            user_id,
            session_id,
            event_type,
            product_id,
            category,
            device_type,
            platform,
            event_timestamp,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            consumed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """

    cursor.execute(
        query,
        (
            event.get("event_id"),
            event.get("user_id"),
            event.get("session_id"),
            event.get("event_type"),
            event.get("product_id"),
            event.get("category"),
            event.get("device_type"),
            event.get("platform"),
            event.get("timestamp"),
            event.get("_kafka_topic"),
            event.get("_kafka_partition"),
            event.get("_kafka_offset"),
            event.get("_consumed_at"),
        ),
    )


def insert_order(cursor, event):

    item = event.get("items", [{}])[0]

    query = """
        INSERT INTO orders (
            order_id,
            customer_id,
            product_id,
            category,
            quantity,
            product_price,
            payment_mode,
            payment_status,
            order_status,
            order_value,
            discount_applied,
            event_timestamp,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            consumed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    cursor.execute(
        query,
        (
            event.get("order_id"),
            event.get("customer_id"),
            item.get("product_id"),
            item.get("category"),
            item.get("qty"),
            item.get("price"),
            event.get("payment_mode"),
            event.get("payment_status"),
            event.get("order_status"),
            event.get("order_value"),
            event.get("discount_applied"),
            event.get("timestamp"),
            event.get("_kafka_topic"),
            event.get("_kafka_partition"),
            event.get("_kafka_offset"),
            event.get("_consumed_at"),
        ),
    )


def insert_inventory(cursor, event):

    query = """
        INSERT INTO inventory (
            inventory_event_id,
            warehouse_id,
            product_id,
            sku,
            stock_change_type,
            quantity,
            current_stock_level,
            event_timestamp,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            consumed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
    """

    cursor.execute(
        query,
        (
            event.get("inventory_event_id"),
            event.get("warehouse_id"),
            event.get("product_id"),
            event.get("sku"),
            event.get("stock_change_type"),
            event.get("quantity"),
            event.get("current_stock_level"),
            event.get("timestamp"),
            event.get("_kafka_topic"),
            event.get("_kafka_partition"),
            event.get("_kafka_offset"),
            event.get("_consumed_at"),
        ),
    )


def insert_logistics(cursor, event):

    query = """
        INSERT INTO logistics (
            shipment_id,
            order_id,
            courier_partner,
            current_status,
            current_location,
            expected_delivery_date,
            event_timestamp,
            kafka_topic,
            kafka_partition,
            kafka_offset,
            consumed_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """

    cursor.execute(
        query,
        (
            event.get("shipment_id"),
            event.get("order_id"),
            event.get("courier_partner"),
            event.get("current_status"),
            event.get("current_location"),
            event.get("expected_delivery_date"),
            event.get("timestamp"),
            event.get("_kafka_topic"),
            event.get("_kafka_partition"),
            event.get("_kafka_offset"),
            event.get("_consumed_at"),
        ),
    )


def main():

    wait_for_kafka()

    mysql_connection = connect_mysql()
    cursor = mysql_connection.cursor()

    topic_list = list(TOPICS.values())

    consumer = KafkaConsumer(
        *topic_list,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(2, 6),
        group_id="sda-assignment2-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    print("Consumer started. Waiting for Kafka messages...")

    try:

        for message in consumer:

            topic = message.topic
            event = message.value

            source = next(
                source
                for source, topic_name in TOPICS.items()
                if topic_name == topic
            )

            event["_kafka_topic"] = topic
            event["_kafka_partition"] = message.partition
            event["_kafka_offset"] = message.offset
            event["_consumed_at"] = time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            if source == "clickstream":

                insert_clickstream(
                    cursor,
                    event
                )

            elif source == "orders":

                insert_order(
                    cursor,
                    event
                )

            elif source == "inventory":

                insert_inventory(
                    cursor,
                    event
                )

            elif source == "logistics":

                insert_logistics(
                    cursor,
                    event
                )

            mysql_connection.commit()

            print(
                f"[MySQL] {source}: "
                f"saved partition={message.partition}, "
                f"offset={message.offset}"
            )

    except KeyboardInterrupt:

        print("\nConsumer stopped.")

    except Error as exc:

        print(f"MySQL error: {exc}")

    finally:

        consumer.close()
        cursor.close()
        mysql_connection.close()


if __name__ == "__main__":
    main()