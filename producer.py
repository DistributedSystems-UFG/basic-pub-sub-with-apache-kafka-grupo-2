from kafka import KafkaProducer
from const import *
import sys
from random import randint

try:
    topics = sys.argv[1:]
except:
    print ('Usage: python3 producer <topics_names>')
    exit(1)
    
producer = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
for i in range(100):
    index = randint(0, len(topics) - 1)
    msg = 'My ' + str(i) + 'st message for topic ' + topics[index]
    print ('Sending message: ' + msg)
    producer.send(topics[index], value=msg.encode())

producer.flush()
