from time import sleep
from json import loads
from kafka import KafkaConsumer
from mysql_connector import insert_into_table, create_table

def main():
    # source topic
    topic = 'second_topic'
    # taget MySQL db/table
    database = "kafka_messages"
    table = "test_table"

    # set up a Kafka consumer
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                            auto_offset_reset='earliest', enable_auto_commit=True, 
                            group_id='my-first-app', value_deserializer=lambda x: loads(x.decode('utf-8')))

    # create table (if not exists) to store messages 
    create_table(database, table)

    # read messages from the topic
    for message in consumer:
        message = message.value
        print("Message recieved: {}. Writing to MySQL".format(message))
        
        # write to mysql table
        insert_into_table(database, table, (message,))

if __name__ == '__main__':
    main()