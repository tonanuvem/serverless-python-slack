import json
import requests

url = "https://hooks.slack.com/services/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"
canal = "#lab-testes"

def handler(event, context):
    print(event)
    if not 'msg' in event['data']:
        return 'Campo vazio : msg'
    #exemplo de uso: kubeless function call slack --data '{"msg":"Serverless é show!"}'
    texto=event['data']['msg']
    print("Texto = "+ texto)
    return postMSG_criada_para_o_slack(texto)
    
def postMSG_criada_para_o_slack(msg):
    # format payload for slack
    sdata = formatForSlack(msg)
    r = requests.post(url, sdata, headers={'Content-Type': 'application/json'})
    if r.status_code == 200:
      print('SUCCEDED: Sent slack webhook')
    else:
      print('FAILED: Send slack webhook')

def formatForSlack(msg):
  payload = {
    "channel":canal,
    "username":'ALUNO_Serverless',
    "text": msg,
    "icon_emoji":':cyclone'
  }
  return json.dumps(payload)
