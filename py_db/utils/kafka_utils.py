from kafka import KafkaProducer, KafkaConsumer
from threading import Thread


def produce_kafka_message(broker, topic, message):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: str(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )

    producer.send(topic, value=message)

    producer.flush()


def register_kafka_listener(BOOTSTRAP_SERVERS, topic, listener):
# Poll kafka
    def poll():
        # Initialize consumer Instance
        consumer = KafkaConsumer(topic, bootstrap_servers=BOOTSTRAP_SERVERS)

        print("About to start polling for topic:", topic)
        consumer.poll(timeout_ms=6000)
        print("Started Polling for topic:", topic)
        for msg in consumer:
            listener(msg)
    print("About to register listener to topic:", topic)
    t1 = Thread(target=poll)
    t1.start()
    print("started a background thread")
