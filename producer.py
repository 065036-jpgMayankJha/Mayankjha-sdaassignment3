import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from config import KAFKA_BOOTSTRAP_SERVERS, TOPICS
from data_generator import GENERATORS

def wait_for_kafka():
    while True:
        try:
            admin = KafkaAdminClient(
               bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
               client_id="sda-assignment2-admin",
               api_version=(2, 6),
            )   

            admin.close()
            print("Kafka is available.")
            return
        except NoBrokersAvailable:
            print("Waiting for Kafka...")
            time.sleep(3)

def create_topics():
    admin = KafkaAdminClient(
       bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
       client_id="sda-assignment2-admin",
       api_version=(2, 6),
   )   
    topics = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in TOPICS.values()
    ]
    try:
        admin.create_topics(new_topics=topics, validate_only=False)
        print("Kafka topics created.")
    except TopicAlreadyExistsError:
        print("Kafka topics already exist.")
    except Exception as exc:
        if "already exists" in str(exc).lower():
            print("Kafka topics already exist.")
        else:
            raise
    finally:
        admin.close()

def main():
    wait_for_kafka()
    create_topics()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(2, 6),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        retries=5,
    )

    print("Producer started. Press Ctrl+C to stop.")

    try:
        while True:
            # Generate one event from each A1 source in every cycle.
            for source, generator in GENERATORS.items():
                event = generator()
                topic = TOPICS[source]
                producer.send(topic, value=event)
                print(f"[{topic}] sent: {event}")

            producer.flush()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
