from time import sleep
from json import dumps
from kafka import KafkaProducer

def main():
    # set up a producer
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                            value_serializer=lambda x: 
                            dumps(x).encode('utf-8'))

    # get messages
    messages = range(3)

    # send messages to kafka
    for message in messages:
        data = {'number' : str(message)+'_sergio'}
        producer.send('second_topic', value=data)

if __name__ == '__main__':
    main()