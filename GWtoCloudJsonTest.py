import paho.mqtt.client as paho
import time
from random import randrange
import json


BROKER = "localhost"
PORT = 1883
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




#for publishing light details to web server
Publisher = publisher('LightDetails')

 
while True:
        Publisher.start()
        rand1=randrange(0,2)
        rand2=randrange(0,2)
        rand3=randrange(0,2)
        rand4=randrange(0,2)
        final_json='''{
        "LightStatusReport" : 
	{
		"GMacId" : "9982AB211321",
		"LightInfo":[{
				"LMacId": "0088A13322BC",
				"Status": "1",
				"Relays": '''+str(rand1)+","+str(rand2)+'''
			},
			{
				"LMacId": "112233445566",
				"Status": "1",
				"Relays": '''+str(rand3)+","+str(rand4)+'''
			}
		]
	}
        }'''
        Publisher.publish(final_json)
        time.sleep(10)
