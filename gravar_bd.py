from kafka import KafkaConsumer
import requests
import json, os

'''
Modelo: 
URLS = {
    "link1": {
        "shorturl": "link1",
        "link": "http://tonanuvem.net",
    },
    "link2": {
        "shorturl": "link2",
        "link": "http://slack.com",
    },
}

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))
'''

def bd(event, context):
    print(event)

    # To consume latest messages and auto-commit offsets
    broker = "kafka.kubeless:9092"
    consumer = KafkaConsumer('urls', bootstrap_servers=[broker])
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))

    # consume earliest available messages, don't commit offsets
    #KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

    # consume json messages
    #KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

    # consume msgpack
    #KafkaConsumer(value_deserializer=msgpack.unpackb)

    # StopIteration if no message after 1sec
    KafkaConsumer(consumer_timeout_ms=1000)
