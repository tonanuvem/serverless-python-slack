from datetime import datetime
from kafka import KafkaConsumer
import requests
import json, os
from pymongo import MongoClient
import urllib.parse

#client = MongoClient("mongodb://localhost:27017/") # Local
usuario = urllib.parse.quote_plus('user')
senha = urllib.parse.quote_plus('password')
server = "mongodb.default.svc.cluster.local:27017"

#'''
#Modelo: 
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
#'''


def read_all():
    dict_urls = [URLS[key] for key in sorted(URLS.keys())]
    urls = jsonify(urls)
    qtd = len(dict_urls)
    print("Read ALL URLS = " + str(urls))
    return alunos

def read_one(key):
    if key in URLS:
        url = URLS.get(key)
        print("Read ONE = " + str(url))
        return url
    else:
        erro = 'Chave não encontrada'
        print(erro)
        return erro

def redirect_link(key):
    if key in URLS:
        url = URLS.get(key)
        link = url['link']
        print("Redirecionando para o link = " + str(link))
        return requests.get(link)
    else:
        erro = 'Chave não encontrada'
        print(erro)
        return erro
    
def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))
    
def get_dict_from_mongodb(db):
    
    # Authentication failed # itens_db = db.clientes.find()
    if not itens_db:
        erro = 'Erro ao conectar ao BD : falha ao ler itens_db'
        print(erro)
        return erro
    print("QTD Itens no DB = " + str(db.clientes.count_documents({})) )
    URLS = {}
    
    for i in itens_db:
            i.pop('_id') # retira id: criado automaticamente pelo mongodb
            item = dict(i)
            URLS[item["shorturl"]] = (i)
            print("URL item [ shorturl ] = " + str(i))
    
    return URLS

def get(event, context):
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
    '''
    #client = MongoClient("mongodb://%s:%s@%s/" % (usuario, senha, server)) # K8S
    client = MongoClient("mongodb://mongo.default:27017")
    db = client.bancodados
    try:
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        #MongoClient(username=usuario, password=senha)
        if not db:
            erro = 'Erro ao conectar ao BD : mongoDB'
            print(erro)
            return erro
        print('Conectado ao BD = ' + str(db))
    except Exception as e:
        print("ERRO ao conectar ao Server (not available)")
    
    try:
        # Decode UTF-8 bytes to Unicode, and convert single quotes 
        # to double quotes to make it valid JSON
        texto=event['data'].decode('utf-8').replace("'", '"')
        print('Texto = '+ texto)
        
        # Se Event_data == vazio --> read_all
        dados = json.loads(texto)
        print('Dados = '+ str(dados))
        if not dados:
          return read_all()
        
        # Se Event_data tiver {"shorturl"} --> read_one
        if 'shorturl' in dados:
          shorturl = dados['shorturl']
          print('shorturl = ' + shorturl)
          return read_one(shorturl)
        
        # Se Event_data tiver string : shorturl --> redirect_link
        
        #URLS = get_dict_from_mongodb(db)
        print("URLS = "+ str(URLS))
        
        #String de dados é a shorturl
        shorturl = dados
        
        if shorturl in URLS and shorturl is not None:
            print("Tentando redirecionar o seguinte item no BD = " + str(shoturl))
            redirect_link(shorturl)
        
    except Exception as e:
        # Decide what to do if produce request failed...
        #print(repr(e))
        #event.extensions.response.statusCode = 400;
        erro = "Erro na function: " + repr(e);
        print(erro)
        return erro
