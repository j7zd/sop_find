from kafka import KafkaProducer, KafkaConsumer
from ast import literal_eval

def produce_kafka_message(broker, topic, message):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: str(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )

    producer.send(topic, value=message)

    producer.flush()


def msg_listener(msg):
    m = msg.value.decode("utf-8")
    return literal_eval(m)
    

class SOP_FIND:
    def __init__(self, servers, usr_id) -> None:
        self._servers = servers
        self._usr_id = usr_id
        self.msg_id = 0

    def get_students_for_teacher(self, teacher_id):
        consumer = KafkaConsumer('responses' + str(self._usr_id), bootstrap_servers=self._servers)
        consumer.poll(timeout_ms=6000)
        produce_kafka_message(self._servers, 'requests', str([self._usr_id, self.msg_id, 'tfetch', teacher_id]))
        self.msg_id += 1
        for msg in consumer:
            d = msg_listener(msg)
            if d[0] == self.msg_id - 1:
                return d[1]