import paho.mqtt.client as paho
import time
import sqlite3
import json


listMID=[]
BROKER = "localhost"
PORT = 1883
status = ''
Input=''
ListRelay=[None]*4

class Queue:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

Queue_Cons=Queue()
      
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
      global Queue_Cons
      def __init__(self,topic):
            self.Input=None
            self.topic=topic
            
            
      
      def on_subscribe(self,client, userdata, mid, granted_qos):
                pass
                
                        
      def on_message(self,client, userdata, msg):
            print('Topic- '+msg.topic+" : "+str(msg.payload))
            if str(msg.topic)=='0088A13322YY':
                  
                  Queue_Cons.enqueue(msg.payload)
            else:
                  self.Input=msg.payload
                
 
      def start(self):
            self.client = paho.Client()
            self.client.on_subscribe = self.on_subscribe
            self.client.on_message = self.on_message
            self.client.connect(BROKER,PORT)
            self.client.subscribe(self.topic, qos=1)
            self.client.loop_start()
            
      def stop(self):    
            self.client.loop_stop()
            
      def get_Input(self):
            return self.Input
      
      def set_Input(self,Input1):
            self.Input = Input1
            
Publisher = publisher('LightRegister')
Publisher.start()

Consumer_LC=consumer("0088A13322YY")
Consumer_LC.start()

Publisher_Details=publisher('LightStatus')
Publisher_Details.start()

Consumer_Response=consumer('RegisterResponse')
Consumer_Response.start()

    
#json for register request
string="""{
            "RegisterLight": {
                  "LightMacId":"0088A13322YY"
              }
      }"""
Publisher.publish(string)

#json for registered device details
def LightDetails(list1):
      jsonArray={}
      jsonArray["LightStatus"]={}
      jsonArray["LightStatus"]["LightMacId"]="0088A13322YY"
      jsonArray["LightStatus"]["Status"]="1"
      jsonArray["LightStatus"]["Relays"]=str(list1)
      
      return jsonArray

def relayArray():
      global ListRelay
      global Queue_Cons
      while (not Queue_Cons.isEmpty()):
            j=json.loads(str(Queue_Cons.dequeue()))
            ListRelay[int(j['Command']['RelayId'])-1]=j['Command']['Level']

FirstJson='''{
                LightStatus:{
                                "LightMacID"="0088A13322YY"
                                "Status"="1"
                                "Relays"='''+str([0,0,None,None])+'''
                            }
            }'''
      

Flag=True
a=''
while True:
      #publishing for registering
      if Flag==True:
            Publisher.publish(string)
            

      #q=Consumer_Response.get_queue()
      stat=Consumer_Response.get_Input()
      if stat:
            j=json.loads(str(stat))
            if(j['RegisterRSP']['Status']=="Success"):
                  Flag=False
                  a=True
                  Publisher_Details.publish(FirstJson)
                  Consumer_Response.stop()
            elif(j['RegisterRSP']['Status']=="SuccessPrev"):
                 Flag=False
                 a=True
                 Consumer_Response.stop()
                 
      if a:
          relayArray()
         
          json1=LightDetails(ListRelay)
          Publisher_Details.publish(json.dumps(json1,sort_keys=True))
      print(Flag)
      time.sleep(10)
