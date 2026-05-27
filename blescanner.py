#!/usr/bin/env python3
'''***************************************************************************
 * 
 * -------------------------------------------------------------------------
 * begin                : 
 * last changes         : 
 * copyright            : (C) N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 * source               :
 ***************************************************************************'''

import asyncio
import math
from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

from Thermobeacon.decode import decodeAdData

class BLEScanner():

    MAX_CALLBACKS = 100
    
    '''***************************************************************************

    ***************************************************************************'''
    def __init__(self, beacons, timeout, logging):
        self.logging   = logging
        self.beacons   = beacons
        self.scantime  = timeout
        self.decoded   = [] 
        self.numbeacons= len(beacons.split(','))
        self.stopEvent = asyncio.Event()        
        self.scanner   = BleakScanner(detection_callback = self.__callback)

    '''***************************************************************************

    ***************************************************************************'''
    async def scan(self):
        self.count = 0
        self.decoded.clear() # clear list of dictionaries
        self.logging.debug(f"BLEScanner(): scanning for devices...")
        self.stopEvent.clear() # we need to reset async event here!
        await self.scanner.start()
        await asyncio.sleep(self.scantime)
        try:
            await self.stopEvent.wait() # cycle until event is set
        finally:
            await self.scanner.stop()

        
        self.logging.debug(f"BLEScanner(): {len(self.decoded)} devices found!")
        
    '''***************************************************************************

    ***************************************************************************'''
    def __callback(self, device: BLEDevice, advData: AdvertisementData):
        mac = device.address.lower()        
        if mac not in self.beacons.lower():
            return # disregard non matching mac addresses

        for d in self.decoded:
            if d["mac"] == mac:
                return  # skip ads already parsed!
        
        msg = advData.manufacturer_data
        for key in msg.keys():
            if len(msg[key]) == 18:
                dict: Dict = decodeAdData(mac, key, msg[key])
                dict["mac"] = mac
                self.decoded.append(dict)
                break

        if len(self.decoded) >= self.numbeacons:
            self.stopEvent.set() # we're definately done here!
            

           
    def getDecoded(self):
        return self.decoded
