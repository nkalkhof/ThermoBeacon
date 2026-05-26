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
import copy
import time
import logging

from Thermobeacon.blescanner import BLEScanner
from Thermobeacon.sample     import Sample
from Thermobeacon.sample     import compareSamples
from Thermobeacon.publisher  import Publisher
from Thermobeacon.decode     import decodeAdData


class Controller():
    
    '''***************************************************************************

    ***************************************************************************'''
    def __init__(self, args, logger):
        self.args      = args
        self.logging   = logger
        self.firstTime = False
        self.scanner: BLEScanner = None
        self.prevSamples = []
        self.samples     = []
        self.starttime   = time.time()         

    '''***************************************************************************

    ***************************************************************************'''
    def __buildSample(self, mac, decoded: dict) -> Sample:
          sample: Sample = Sample()
          sample.btmac       = mac
          sample.temperature = decoded["temperature"]
          sample.humidity    = decoded["humidity"]
          self.samples.append(sample)
          for b in self.args.beacons.split(','):
            if(decoded["mac"].casefold() in b):
              label, macaddr = b.split("=", maxsplit = 1)
              sample.label = label
              break
          return sample


    '''***************************************************************************
    # accumulate mac addresses, skip already received!
    ***************************************************************************'''
    async def test01(self, label, mac, data: bytearray) -> bool:
        if mac.casefold() not in self.args.beacons.casefold():
            return # disregard non matching mac addresses

        decoded = False
        for s in self.samples:
            if s.btmac.casefold() == mac.casefold():
                decoded = True
                break # already decodecd!
        
        if decoded is False: # not already decodecd!
            for key in data.keys():
                if len(data[key]) == 18:
                    dict: Dict = decodeAdData(mac, key, data[key])
                    dict["mac"] = mac
                    self.__buildSample(mac, dict)
                    decoded = True
                    break
            if decoded is False:
                return

        if (time.time() - self.starttime) < self.args.scantime:
            return False
        
        self.starttime = time.time()

        self.logging.debug(f"===============> found {len(self.samples)} devices!!!!")
        for s in self.samples:
            logging.debug(s.__str__())

        # do the deed
        await self.publish()            

        self.logging.debug(f"finished in {time.time() - self.starttime} seconds!")
        return True

        
    '''***************************************************************************

    ***************************************************************************'''
    async def connect(self):
        self.scanner: BLEScanner = BLEScanner(self.args.beacons, 
        timeout = self.args.scantime, logging = self.logging)


    '''***************************************************************************

    ***************************************************************************'''
    async def scan(self):
        await self.scanner.scan()

        decoded = self.scanner.getDecoded()
        if not decoded: # empty dict, device not found, omit!
            return

        for d in decoded:
          self.samples.append(self.__buildSample(d["mac"], d))
            
        for s in self.samples:
            logging.debug(s.__str__())

    '''***************************************************************************

    ***************************************************************************'''
    async def publish(self):        
        if len(self.samples) == 0:
            return
            # todo sample check different!
            # cycle through EVERY sample and check values!
            # if new samples > prev, publish also!
        if self.firstTime == True or self.prevSamples != self.samples:
                #or compareSamples(self.samples, self.prevSamples):
            await Publisher(self.args, self.logging).doPublish(self.samples)
            self.prevSamples = copy.deepcopy(self.samples)
            self.firstTime == False
        else:
            self.logging.debug(f"publish(): samples identical, omitting publish!")
        self.samples.clear()
 
