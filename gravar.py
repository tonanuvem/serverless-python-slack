from kafka import KafkaConsumer
import requests
import json, os
from pymongo import MongoClient

#client = MongoClient("mongodb://localhost:27017/") # Local
client = MongoClient("mongodb://mongo.default:27017/") # Docker
db = client.tododb

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
'''

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))
    
def get_dict_from_mongodb():
    itens_db = db.clientes.find()
    URLS = {}
    for i in itens_db:
            i.pop('_id') # retira id: criado automaticamente pelo mongodb
            item = dict(i)
            URLS[item["shorturl"]] = (i)
    return URLS

def bd(event, context):
    print(event)
    if not 'shorturl' in event['data']:
        return 'Campo vazio : shorturl'
    if not 'link' in event['data']:
        return 'Campo vazio : link'
    if not db:
        return 'Erro ao conectar ao BD : mongoDB'
    try:
        texto=str(event['data'])
        print('Texto = '+ texto)
        shorturl = event['data']['shorturl']
        link = event['data']['link']
        URLS = get_dict_from_mongodb()
        if shorturl not in URL and shorturl is not None:
            item = {
                "shorturl": shorturl,
                "link": link,
                "timestamp": get_timestamp(),
            }
            db.clientes.insert_one(item)
            return shorturl + " criada com sucesso"
        else:
            return shorturl + " ja existe"
    except Exception as e:
        # Decide what to do if produce request failed...
        #print(repr(e))
        #event.extensions.response.statusCode = 400;
        return "Erro na function: " + repr(e);

'''
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
'''
