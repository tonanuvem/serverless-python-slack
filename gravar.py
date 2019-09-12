from kafka import KafkaConsumer
import requests
import json, os
from pymongo import MongoClient

#client = MongoClient("mongodb://localhost:27017/") # Local
client = MongoClient("mongodb://mongodb.default:27017/") # Docker
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
    print("Itens no DB = " + itens_bd)
    URLS = {}
    for i in itens_db:
            i.pop('_id') # retira id: criado automaticamente pelo mongodb
            item = dict(i)
            URLS[item["shorturl"]] = (i)
    return URLS

def bd(event, context):
    print("Evento recebido = " + str(event))
    '''
    if not 'shorturl' in event['data']:
        erro = 'Campo vazio : shorturl'
        print(erro)
        return erro
    if not 'link' in event['data']:
        erro = 'Campo vazio : link'
        print(erro)
        return erro
    if not client:
        erro = 'Erro ao conectar ao BD : mongoDB'
        print(erro)
        return erro
    '''
    print('cheguei aqui')
    
    try:
        # Decode UTF-8 bytes to Unicode, and convert single quotes 
        # to double quotes to make it valid JSON
        texto=event['data'].decode('utf-8').replace("'", '"')
        print('Texto = '+ texto)
        dados = json.loads(texto)
        print('Dados = '+ str(dados))
        shorturl = dados['shorturl']
        link = dados['link']
        print('shorturl = ' + shorturl + " / "+ 'link = ' + link)
        
        URLS = get_dict_from_mongodb()
        '''
        if shorturl not in URL and shorturl is not None:
            item = {
                "shorturl": shorturl,
                "link": link,
                "timestamp": get_timestamp(),
            }
            print("Tentando inserir o seguinte item no BD = " + str(item))
            db.clientes.insert_one(item)
            msg = shorturl + " criada com sucesso"
            print(msg)
            return msg
        else:
            msg = shorturl + " ja existe"
            print(msg)
            return msg
        '''
    except Exception as e:
        # Decide what to do if produce request failed...
        #print(repr(e))
        #event.extensions.response.statusCode = 400;
        erro = "Erro na function: " + repr(e);
        print(erro)
        return erro

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
