from flask import Flask, abort, request 
import paho.mqtt.client as mqtt
import time
import mysql.connector
from mysql.connector import errorcode
import json

BROKER = "localhost"

app = Flask(__name__)

lmacid = ""
relayid = ""
level = ""

dbconfig = {
  'user': 'root',
##  'password': '*1Passwd',
##  'host': '10.169.152.42',
  'password': 'root',
  'host': '127.0.0.1',
  'database': 'lightingdb',
}

LightCommandRspSuccess = {"LightControlRSP" : { "Status" : "Success" } }
LightCommandRspFailure = {"LightControlRSP" : { "Status" : "Failure" } }


try:
  cnx = mysql.connector.connect(pool_name = "mypool", **dbconfig)
   
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Mysql Connection failed")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exist")
  else:
    print(err)

client = mqtt.Client()

@app.route('/LightCommand', methods=['POST']) 
def LightControl():
    if not request.json:
        abort(400)
    command_data = json.loads(request.json)
##    test = "{\"LightControl\":{\"Command\":[{\"LMacId\":\"0088A13322BC\",\"RelayId\":\"1\",\"Level\":\"00\"}]}}"
##    command_data = json.loads(test)
    cursor = cnx.cursor()
    for r in command_data['LightControl']['Command']:
        lmacid = r['LMacId']
        relayid = r['RelayId']
        level = r['Level']
        jsonArray = {}
        jsonArray['Command'] = {}
        jsonArray['Command']['LMacId']=str(lmacid)
        jsonArray['Command']['RelayId'] = str(relayid)
        jsonArray['Command']['Level']=str(level)
        json.dumps(jsonArray)
##        lmacid = '0088A13322BC'
        cursor.execute('''SELECT gwymacid FROM lightingnodes WHERE lightmacid=%s''',(lmacid,))
        for gmacid in cursor:
          client.connect(BROKER, 1883)
          client.publish(str(gmacid), json.dumps(jsonArray, sort_keys=True))
          client.disconnect()
    cursor.close()
    return json.dumps(LightCommandRspSuccess)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
