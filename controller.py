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

from thermobeacon.scanner   import BLEScanner
from thermobeacon.sample    import Sample
from thermobeacon.sample    import compareSamples
from thermobeacon.publisher import Publisher
from thermobeacon.decode    import decodeAdData

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
          sample             = Sample()
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
    def run_external(self, device, mac, data: bytearray):
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

        delta = time.time() - self.starttime
        if delta >= self.args.interval:
            self.logging.debug(f"======================> firing up thermobeacon query "\
                               f"<======================") 
            self.logging.debug(f"found {len(self.samples)} devices in "\
                                f"{delta:.2f} seconds")
            for s in self.samples:
                self.logging.debug(s.__str__())
            self.publish()            
            self.starttime = time.time()

        
    '''***************************************************************************

    ***************************************************************************'''
    def connect(self):
        self.scanner: BLEScanner = BLEScanner(self.args.beacons, 
        timeout = self.args.scantime, logging = self.logging)


    '''***************************************************************************

    ***************************************************************************'''
    async def scan(self):
        await self.scanner.scan()
        self.samples.clear()

        decoded = self.scanner.getDecoded()
        if not decoded: # empty dict, device not found, omit!
            return

        for d in decoded:
            self.__buildSample(d["mac"], d)
            
        for s in self.samples:
            self.logging.debug(s.__str__())

    '''***************************************************************************

    ***************************************************************************'''
    def publish(self):        
        if len(self.samples) == 0:
            return
        if compareSamples(self.samples, self.prevSamples) is False:
            Publisher(self.args, self.logging).doPublish(self.samples)
            self.prevSamples = copy.deepcopy(self.samples)
        else:
            self.logging.debug(f"samples identical, omitting publish!")
        self.samples.clear()
 
