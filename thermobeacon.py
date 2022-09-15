#!/usr/bin/env python3
"""
/***************************************************************************
 *
 * 
 *
 * -------------------------------------------------------------------------
 * 
 * -------------------------------------------------------------------------
 * begin                : Sept 01 2022
 * last changes         : Sept 03 2022
 * copyright            : (C) 2022 by N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 ***************************************************************************/
"""
# @see: https://novelbits.io/bluetooth-low-energy-advertisements-part-1/
import signal
import sys
import time
import math
import asyncio
import paho.mqtt.client as mqtt
from time import gmtime, strftime
from bleak import BleakScanner
#from thermo_sample import ThermoSample

SENSORS = {"6f:15:00:00:00:42": "livingroom" ,"6f:15:00:00:0c:b1" : "bedroom"}
MQTT_BROKER_URL = "localhost"
MQTT_TOPICS = [("livingroom_temp",0),("bedroom_temp",0),("livingroom_hum",0),("bedroom_hum",0)]
SAMPLE_INTERVAL = 30
DISCOVERY_TIME = 4

class ThermoSample:
  def __init__(self, mac, location, 
               battery = math.nan,
               temperature = math.nan, 
               humidity = math.nan):
    self.mac         = mac
    self.location    = location
    self.battery     = battery,
    self.temperature = temperature
    self.humidity    = humidity
     
  def __str__(self):
    return ("========>{0},{1}<========\n"
      "temperature:\t{2:.2f}°C\nhumidity:\t{3:.2f}%\n".
      format(self.location, self.mac, self.temperature, self.humidity))    
  

scanner = BleakScanner()    
mqttc = mqtt.Client()
samples : ThermoSample = []
prev_samples : ThermoSample = []

'''
ADVERTISING MESSAGES
Decode Manufacturer specific data from BLE Advertising message
Message length: 18 bytes
bytes | content
========================================================
00-01 | code
02-02 | 00 ?
03-03 | 0x80 if Button is pressed else 00?
04-08 | mac address
08-10 | battery level: seems that 3400 = 100% (3400 mV, not quite sure)
10-12 | temperature
12-14 | humidity
14-18 | uptime: seconds since the last reset
'''
def decode_temp_and_hum(b:bytes) -> float:
    result = int.from_bytes(b, byteorder='little')/16.0
    if result > 4000:
        result -= 4096
    return result

def do_decode(mac, location, key, bvalue) -> ThermoSample:
    if key not in [0x10, 0x11]:
        raise ValueError()
    this_sample = ThermoSample(
        mac         = mac,
        location    = location,
        battery     = int.from_bytes(bvalue[8:10], byteorder='little'),
        temperature = decode_temp_and_hum(bvalue[10:12]),
        humidity    = decode_temp_and_hum(bvalue[12:14])
        )       
    return this_sample
    

def detection_callback(device, advertisement_data):
    mac = device.address.lower()
    if mac not in SENSORS:
        return # we're done here!   
    for i in range(len(samples)): # already sampled!
        if samples[i].mac == mac:
            return                
    msg = advertisement_data.manufacturer_data
    for key in msg.keys():
        if len(msg[key]) == 18:
            samples.append(do_decode(mac, SENSORS[mac], key, msg[key]))
            break

def publish():
    anychange = False
    for i in range(len(samples)):
        if len(prev_samples) < 2 or \
            samples[i].temperature != prev_samples[i].temperature or \
            samples[i].humidity != prev_samples[i].humidity:
            anychange = True
            
    if anychange is True:
        prev_samples.clear()    
        print('connecting to broker and publishing...', end='')
        mqttc.connect(MQTT_BROKER_URL, port=1883, keepalive=60, bind_address="")
        mqttc.subscribe(MQTT_TOPICS)
        for i in range(len(samples)):
            mqttc.publish("{}_temp".format(samples[i].location), f"{samples[i].temperature:.2f}")
            mqttc.publish("{}_hum".format(samples[i].location), f"{samples[i].humidity:.2f}")
            prev_samples.append(samples[i])            
        mqttc.disconnect()
        print('done')
    else:
        print("no changes in samples compare to previous, omitting publish")                    
            
                  
def signal_handler(signal, frame):
  print('disconnecting from broker...')
  mqttc.disconnect()
  print('stopping scanner...')
  scanner.stop()
  sys.exit(0)

async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    
    scanner.register_detection_callback(detection_callback)    
    print('starting discovery...')   
    await scanner.discover()    
    while(True):
        starting_time = time.time()       
        samples.clear()
        print('starting scan @', strftime("%Y-%m-%d %H:%M:%S", gmtime()), '...')
        await scanner.start()
        await asyncio.sleep(DISCOVERY_TIME)
        await scanner.stop()
        print('scan completed')
        for i in range(len(samples)):
            print(samples[i])             
        publish()            
        time_delta = SAMPLE_INTERVAL - (time.time() - starting_time)
        if time_delta > 0:
            print('sleeping for {0:2.1f} seconds...'.format(time_delta))
            await asyncio.sleep(time_delta)
            
asyncio.run(main())
