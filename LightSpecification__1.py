import paho.mqtt.client as paho
import time
import re
import json
import sqlite3
from random import randrange
import ast

BROKER = "localhost"
BROKER_Nithin="10.170.46.27"
PORT = 1883

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

Queue_Control=Queue()
      
class publisher:
      def __init__(self,topic,broker):
            self.topic=topic
            self.broker=broker
            
      def on_publish(self,client, userdata, mid):
            print("mid: "+str(mid))
            
      def start(self):
            self.client = paho.Client()
            self.client.on_publish = self.on_publish
            self.client.connect(self.broker,PORT)
            self.client.loop_start()

      def publish(self,status):
            (rc, mid) = self.client.publish(self.topic, str(status), qos=1)
      def stop(self):    
            self.client.loop_stop()

class consumer:
      global Queue_Control
      def __init__(self,topic,broker):
 
            self.Input=None
            self.topic=topic
            self.broker=broker
      
      def on_subscribe(self,client, userdata, mid, granted_qos):
                pass
                
      def on_message(self,client, userdata, msg):
            x=''
            Id=''
            Stat=''
            R1=''
            R2=''
            R3=''
            R4='',
            list1=''
            RelayList=[]
            listIds=[]
            try:
                  conn = sqlite3.connect('/Python27/SQLite/gwy2.db')
         
                  print('Topic- '+msg.topic+" : "+str(msg.payload))
                  j=json.loads(str(msg.payload))

                  for key in j:
                        x=key
                 
                  if(x=='RegisterLight' and len(j[x])==1):
                        
                        c=conn.cursor()
                        Ids=c.execute("Select lightmacid from lightingnodes")
                        for id in Ids:
                               listIds.append(id[0])
                        Id=str(j['RegisterLight']['LightMacId'])
                        Data=[Id]
                        if(Id not in listIds):
                              query="insert into lightingnodes (lightmacid) values(?)"
                              c.execute(query,Data)
                              self.Input = 'Registered'
                        elif(Id in listIds):
                              self.Input='RegisteredPrev'
                        else:
                              self.Input='Error'
                        conn.commit()
                        conn.close()
                  
                  #For updating in the Lights database
                  elif (x=='LightStatus' and len(j[x])==3):
                        RelayCount=0
                        c=conn.cursor()
                        
                        Id=j['LightStatus']['LightMacId']
                        Stat=int(j['LightStatus']['Status'])

                        #No of active relay counts
                        liist=str(j['LightStatus']['Relays'])
                        list1=ast.literal_eval(liist)
                        

                        #storing relayinfo into a list
                        for i in list1:
                              RelayList.append(i)
                      
                        #counting active relays
                        queryRC=("select count(*) from relayinfo where lightmacid=?")
                        data=[Id]
                        rc=c.execute(queryRC,data)
                        for i in rc:
                              RelayCount=i[0]
                      
                        Ids=c.execute("Select lightmacid from lightingnodes") 
                        #listIds will contain all the IDs regestered             
                        for id in Ids:
                               listIds.append(id[0])
                                  
                        if(Id in listIds):
                              #lightingnodes table
                              query1='''update lightingnodes set status=?,
                              activerelaycount=? where lightmacid=?'''
                              data1=[Stat,RelayCount,Id]
                              c.execute(query1,data1)

                              query_c='select count(*) from relayinfo where lightmacid=?'
                              data2=[Id]
                              countDB=c.execute(query_c,data2)
                              count=0
                              for i in countDB:
                                    count=i[0]
                              #relayinfo table
                              if(RelayCount != count):
                                    for i in range(len(RelayList)):
                                          id1=i+1
                                          query_21='''insert into relayinfo(relayid,status,latlong,
                                          description,lightmacid)values(?,?,'5.5,6.7','yo',?)'''
                                          data2=[id1,RelayList[i],Id]
                                          c.execute(query_21,data2)
                              else:
                                    print(RelayList)
                                    for i in range(len(RelayList)):
                                       if RelayList[i]!=None:
                                                id1=i+1
                                                query_22='''update relayinfo set status=?,latlong='5.3,7.8',
                                                description='tt' where lightmacid=? and relayid=?'''
                                                data2=[str(RelayList[i]),Id,id1]
                                                c.execute(query_22,data2)
                              conn.commit()
                              conn.close()
                        else:
                              print('MacId not registered')
                  elif (x=='Command'):
                        List=[]
                        array=j[x]
                        lmacid=array['LMacId']
                        relayid=array['RelayId']
                        level=array['Level']
                        List.extend([str(lmacid),str(relayid),str(level)])
                       
                        Queue_Control.enqueue(List)
                        
            except sqlite3.Error, e:
                  print("Error %s:" % e.args[0])               

                  
      def start(self):
            self.client = paho.Client()
            self.client.on_subscribe = self.on_subscribe
            self.client.on_message = self.on_message
            self.client.connect(BROKER,PORT)
            self.client.subscribe(self.topic, qos=1)
            self.client.loop_start()
            
      def get_Input(self):
            return self.Input
      
      def set_Input(self,Input1):
            self.Input = Input1

         
def process_queue(Queue):
    while(not Queue.isEmpty()):
        Data_Serv=Queue.dequeue()
        Queue_Control.dequeue()
        Publisher_LC=publisher(str(Data_Serv[0]),BROKER)
        Publisher_LC.start()
        
        json='''{"Command":{"RelayId":'''+str(Data_Serv[1])+''',"Level":'''+str(Data_Serv[2])+'''}}'''
        Publisher_LC.publish(json)
        Publisher_LC.stop()
            
Consumer_Register = consumer('LightRegister',BROKER)
Consumer_Details=consumer('LightStatus',BROKER)
Publisher_Response = publisher('RegisterResponse',BROKER)
Consumer_Serv=consumer('AABBCCDDEEFF',BROKER_Nithin)

Consumer_Register.start()
Publisher_Response.start()
Consumer_Details.start()
Consumer_Serv.start()

Data_Serv=Consumer_Serv.get_Input()
while True:
    reg=str(Consumer_Register.get_Input())

    #newly registered
    if reg=='Registered':
          response_json='''{
          "RegisterRSP": {
		"Status": "Success"
	  }
          }'''
          Publisher_Response.publish(response_json)
          Publisher_Response.stop()

    #previously registered
    elif reg=='RegisteredPrev':
        response_json='''{
          "RegisterRSP": {
		"Status": "SuccessPrev"
	  }
          }'''
        Publisher_Response.publish(response_json)
        Publisher_Response.stop()
        
    Queue_Child=Queue_Control
    process_queue(Queue_Child)
    time.sleep(10)

