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
from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

from decode import decodeAdData

class BLEScanner():
    '''***************************************************************************

    ***************************************************************************'''
    def __init__(self, beacons, logging):
        self.logging = logging
        self.beacons = beacons
        self.timeout = 5
        self.decoded = [] 

    '''***************************************************************************

    ***************************************************************************'''
    async def start(self):
        self.decoded.clear() # clear list of dictionaries
        self.logging.debug(f"scanning for devices...")
        devices = await BleakScanner.discover(timeout = self.timeout, return_adv = True)
        for address, (device, advData) in devices.items():
            mac = device.address.lower()
            # disregard non matching mac addresses
            if mac not in self.beacons.lower():
                continue
            # skip ads already parsed!
            for d in self.decoded:
                if d["mac"] == mac:
                    continue
            
            msg = advData.manufacturer_data
            for key in msg.keys():
                if len(msg[key]) == 18:
                    decode: Dict = decodeAdData(mac, key, msg[key])
                    decode["mac"] = mac
                    self.decoded.append(decode)
                    break


    def getDecoded(self):
        return self.decoded

    '''***************************************************************************

    ***************************************************************************'''
    async def __detection_callback(self, device: BLEDevice, 
                advData: AdvertisementData):
        mac = device.address.lower()
        # disregard non matching mac addresses
        if mac not in self.beacons.lower():
            return
        # skip ads already parsed!
        for d in self.decoded:
            if d["mac"] == mac:
                return
        
        msg = advData.manufacturer_data
        for key in msg.keys():
            if len(msg[key]) == 18:
                dict: Dict = decodeAdData(mac, key, msg[key])
                dict["mac"] = mac
                self.decoded.append(dict)
                break
            