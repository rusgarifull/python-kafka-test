from time import sleep
from json import loads
from kafka import KafkaConsumer
from mysql_connector import insert_into_test_table

def main():
    # set up a consumer
    topic = 'second_topic'
    consumer = KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                            auto_offset_reset='earliest', enable_auto_commit=True, 
                            group_id='my-first-app',
                            value_deserializer=lambda x: loads(x.decode('utf-8')))

    # read messages from the topic
    for message in consumer:
        try:
            # Read messages from the topic and write to mysql table
            message = message.value
            print("Message recieved: {}. Writing to MySQL".format(message))
            insert_into_test_table((message,))
            #print('{} added to {}'.format(message, collection))

        except Exception as e:
            print("Error: ", e)
if __name__ == '__main__':
    main()