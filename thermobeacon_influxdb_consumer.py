"""
/***************************************************************************
 *
 * 
 *
 * -------------------------------------------------------------------------
 * MQTT subscriber - Listen to a topic and sends data to InfluxDB
 * -------------------------------------------------------------------------
 * begin                : Sept 01 2022
 * last changes         : Sept 03 2022
 * copyright            : (C) 2022 by N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 ***************************************************************************/
"""
import signal
import sys
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point

# InfluxDB config
INFLUX_ORG = "Hasisbuffen"
INFLUX_BUCKET = 'temptest'

# MQTT broker config
MQTT_BROKER_URL = "127.0.0.1"
MQTT_TOPICS = [("livingroom_temp",0),("bedroom_temp",0),("livingroom_hum",0),("bedroom_hum",0)]

mqttc = mqtt.Client()
mqttc.connect(MQTT_BROKER_URL)

client = InfluxDBClient(url='http://127.0.0.1:8086',
    token='5opyNFxEud0drGuzK0Pu9iH-a6fvkLlS5uDyZ4C8NBriKA1rNYMOS8Wmkx5qXzlLzTk9Jo2BEs49rxx77GyyYg==', 
    org=INFLUX_ORG)

write_api = client.write_api()
       
def signal_handler(signal, frame):
  print('disconnecting from broker...')
  if mqttc.is_connected:
    mqttc.disconnect()
  sys.exit(0)
  
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)
    
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))    
    client.subscribe(MQTT_TOPICS) # subscribe to a topic

def on_disconnect(client, userdata, flags, rc):
    print("disconnected")    
    
data = []
def on_message(client, userdata, msg):
    print('received message topic: ' + msg.topic + " payload: " +str(msg.payload))
    point = Point("temperature").field(msg.topic, float(msg.payload))    
    write_api.write(bucket=INFLUX_BUCKET, record=point)

## register callbacks and start MQTT client on script invoke
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.loop_forever()
