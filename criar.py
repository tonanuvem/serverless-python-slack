from kafka import KafkaProducer
from kafka.errors import KafkaError
from flask import make_response, abort
#from datetime import datetime
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
''''

def create(msg):
    try:
        texto = msg.get("texto", None)
        topico = "urls"
        broker = "kafka.kubeless:9092" #os.environ['HOST'] + ":" + os.environ['PORTA'] #"192.168.10.133:9092"
        print(broker)

        # --------
        # USAGE: https://kafka-python.readthedocs.io/en/master/usage.html
        producer = KafkaProducer(bootstrap_servers=[broker])

        # Asynchronous by default
        future = producer.send(topico, texto.encode('utf-8'))
        # Block for 'synchronous' sends
        record_metadata = future.get(timeout=10)
        
    except Exception as e:
        # Decide what to do if produce request failed...
        print(repr(e))
        event.extensions.response.statusCode = 400;
        return err;

    # Successful result returns assigned partition and offset
    print ('Sucesso no envio. Topico: '+str(record_metadata.topic)+' Particao :' + str(record_metadata.partition) + ' Offset: ' + str(record_metadata.offset))
    event.extensions.response.statusCode = 201;
    return article;
