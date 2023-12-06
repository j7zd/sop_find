from time import sleep
from os import getenv
from utils.kafka_utils import produce_kafka_message, register_kafka_listener
from utils.table_classes import Teacher, StudentSOP, STLinker, Database
from ast import literal_eval
from kafka import KafkaConsumer

USERS = [0]

# create database
database_host = getenv('MYSQL_HOST')
database_username = 'root'
database_password = getenv('MYSQL_ROOT_PASSWORD')
database_name = getenv('MYSQL_DATABASE')

while True:
    try:
        database = Database(database_host, database_username, database_password, database_name)
        break
    except:
        print("Database not ready yet")
        sleep(1)

database_name = 'test'

#create database
database.create_database(database_name)
#use database
database.use_database(database_name)

#create tables
Teacher.create_teacher_table(database)
StudentSOP.create_student_table(database)
STLinker.create_st_table(database)

broker_address = getenv('KAFKA_BROKER')
kafka_topic = 'requests'

while True:
    try:
        consumer = KafkaConsumer(group_id='test', bootstrap_servers=broker_address)
        break
    except:
        print("Kafka not ready yet")
        sleep(1)


def process_tfetch(data):
    return STLinker.get_students_for_teacher(database, data)
    

def process_request(msg):
    d = msg.value.decode("utf-8")
    d = literal_eval(d)
    try:
        usr_id = d[0]
        if usr_id not in USERS:
            raise Exception("Invalid user id")
        msg_id = d[1]
        msg_type = d[2]
        msg_data = d[3]
    except:
        return
    
    msg = 'Error: Invalid message type'

    if msg_type == 'tfetch':
        msg = process_tfetch(msg_data)
    
    produce_kafka_message(broker_address, 'responses' + str(usr_id), str([msg_id, msg]))


if __name__ == '__main__':
    register_kafka_listener(broker_address, kafka_topic, process_request)