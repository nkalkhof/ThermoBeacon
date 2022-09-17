#!/usr/bin/env python3
'''***************************************************************************
 * @see: https://novelbits.io/bluetooth-low-energy-advertisements-part-1/
 * -------------------------------------------------------------------------
 * begin                : Sept 01 2022
 * last changes         : Sept 03 2022
 * copyright            : (C) 2022 by N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 **************************************************************************'''
import signal
import time
import math
import asyncio
import sys
import time
import logging
from bleak import BleakScanner
from influxdb_client import InfluxDBClient, Point

# InfluxDB config and instance
INFLUX_ORG    = "Hasisbuffen"
INFLUX_BUCKET = "thermobeacon"
INFLUX_URL    = "http://127.0.0.1:8086"
INFLUG_TOKEN  = "5opyNFxEud0drGuzK0Pu9iH-a6fvkLlS5uDyZ4C8NBriKA1rNYMOS8Wmkx5qXzlLzTk9Jo2BEs49rxx77GyyYg=="
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUG_TOKEN, org=INFLUX_ORG)
influx_write_api = influx_client.write_api()            

SENSORS = {"6f:15:00:00:00:42": "livingroom" ,"6f:15:00:00:0c:b1" : "bedroom"}
SAMPLE_INTERVAL = 30
DISCOVERY_TIME  =  5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/tmp/thermobeacon.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

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
    return ("\n========>{0},{1}<========\n"
      "temperature:\t{2:.2f}°C\nhumidity:\t{3:.2f}%".
      format(self.location, self.mac, self.temperature, self.humidity))    

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
    if len(prev_samples) < 2:
        anychange = True
    else:
        for i in range(len(samples)): # todo: match order of samples!!!!
            compare = None            
            for j in range(len(prev_samples)):
              if samples[i].mac == prev_samples[j].mac:
                  compare = prev_samples[j]
                  break
            if compare is not None and samples[i].temperature != compare.temperature:
                anychange = True
                break
                
    if anychange is True:
        prev_samples.clear()    
        logging.info('publishing to bucket {0}...'.format(INFLUX_BUCKET))
        for i in range(len(samples)):            
            try:
                point = Point("temperature").field("{}_temp".
                        format(samples[i].location), samples[i].temperature)                
                influx_write_api.write(bucket=INFLUX_BUCKET, record=point)
                point = Point("temperature").field("{}_hum".
                        format(samples[i].location), samples[i].humidity)
                influx_write_api.write(bucket=INFLUX_BUCKET, record=point)
                prev_samples.append(samples[i])            
            except Exception as e:
                logging.warning("publishing failed with {0}!".format(str(e)))                    
    else:
        logging.info("no changes in samples compare to previous, omitting publish")                    
            
            
scanner = BleakScanner()         
                  
def signal_handler(signal, frame):
  logging.info('stopping scanner...')
  scanner.stop()
  logging.info('closing database connection...')
  influx_client.close()
  exit(1)

async def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    
    scanner.register_detection_callback(detection_callback)    
    logging.info('starting discovery...')   
    await scanner.discover()    
    while(True):
        starting_time = time.time()       
        samples.clear()
        logging.info('starting scan...')
        await scanner.start()
        await asyncio.sleep(DISCOVERY_TIME)
        await scanner.stop()
        logging.info('scan completed')
        for i in range(len(samples)):
            logging.info(str(samples[i]))
        publish()            
        time_delta = SAMPLE_INTERVAL - (time.time() - starting_time)
        if time_delta > 0:
            logging.info('sleeping for {0:2.1f} seconds...'.format(time_delta))
            await asyncio.sleep(time_delta)
            
asyncio.run(main())
