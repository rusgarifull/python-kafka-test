from time import sleep
from json import dumps
from kafka import KafkaProducer

def main():
    # set up a producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))

    # get messages
    messages = range(6,8)

    # send messages to kafka
    topic = 'second_topic' # kafka topic to send data to
    for message in messages:
        data = str(message)+'_message'
        producer.send(topic, value=data)
        sleep(3)
    
    # close producer
    producer.close()
if __name__ == '__main__':
    main()