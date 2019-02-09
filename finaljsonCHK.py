import paho.mqtt.client as paho
import time
import re
import json
import sqlite3
from uuid import getnode as get_mac

BROKER = "localhost"
PORT = 1883

#macid of the gateway
macid=get_mac()

class publisher:
	def __init__(self,topic):
		self.topic=topic
		
	def on_publish(self,client, userdata, mid):
    		print("mid: "+str(mid))
    		
	def start(self):
		self.client = paho.Client()
		self.client.on_publish = self.on_publish
		self.client.connect(BROKER,PORT)
		self.client.loop_start()

	def publish(self,status):
		(rc, mid) = self.client.publish(self.topic, str(status), qos=1)
	def stop(self):    
		self.client.loop_stop()

class consumer:
        def __init__(self,topic):
                self.status = None
                self.topic=topic
        def on_subscribe(self,client, userdata, mid, granted_qos):
                pass
        def on_message(self,client, userdata, msg):
                self.status = msg.payload
                print("Setting status to "+self.status)
        def start(self):
                self.client = paho.Client()
                self.client.on_subscribe = self.on_subscribe
                self.client.on_message = self.on_message
                self.client.connect(BROKER,PORT)
                self.client.subscribe(SUBSCRIBE_TOPIC, qos=1)
                self.client.loop_start()
        def get_status(self):
                return self.status
        def set_status(self,status):
                self.status = status
#for registration
Publisher_Reg=publisher('Registration')

#for publishing light details to web server
Publisher = publisher('LightDetails')

#for consuming acknowledgement from the server
#Consumer = consumer('Acknowledge')

Reg_Data={}
Reg_Data["RegisterGateway"]={}
Reg_Data["RegisterGateway"]["GMacId"]=str(macid)



while True:
        Publisher_Reg.start()
        Publisher_Reg.publish(json.dumps(Reg_Data,sort_keys=True))
        Publisher.start()
        #Consumer.start()
        Ack = 'Yes'
        if Ack=='Yes':
                conn = sqlite3.connect('/Python27/SQLite/gwy2.db')
                conn.row_factory=sqlite3.Row
                c=conn.cursor()

                c.execute('''select l.lightmacid as LMacID,l.status as Status,group_concat(r.status) as Relays
                from lightingnodes l inner join relayinfo r
                on l.lightmacid=r.lightmacid group by  l.lightmacid''')

                recs=c.fetchall()
                

                r =[ dict(rec) for rec in recs ]
                
                #rows_json = 
                final_json={}
                final_json["LightStatusReport"]={}
                final_json["LightStatusReport"]["GMacId"]=str(macid)
                final_json["LightStatusReport"]["LightInfo"]=str(json.dumps(r))
                
                
                json.dumps(final_json)
                Publisher.publish(final_json)
                print(final_json)
        time.sleep(10)
