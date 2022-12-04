from kafka import KafkaConsumer
from const import *
import sys

consumer = KafkaConsumer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])
try:
  topics = sys.argv[1:]
except:
  print ('Usage: python3 consumer <topics_names>')
  exit(1)
  
consumer.subscribe(topics)
for msg in consumer:
    print (msg.value)
