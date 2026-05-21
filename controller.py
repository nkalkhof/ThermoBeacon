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
import signal
import copy
import asyncio
import logging

from blescanner import BLEScanner
from sample import Sample
from publisher import Publisher

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

        
    '''***************************************************************************

    ***************************************************************************'''
    async def connect(self):
        self.scanner: BLEScanner = BLEScanner(self.args.beacons, logging = self.logging)


    '''***************************************************************************

    ***************************************************************************'''
    async def read(self):
        await self.scanner.start()
        await asyncio.sleep(self.args.interval)

        decoded = self.scanner.getDecoded()
        if not decoded: # empty dict, device not found, omit!
            return

        self.logging.debug(f"read(): {len(decoded)} devices found!")

        for d in decoded:
          sample: Sample = Sample()
          sample.temperature = d["temperature"]
          sample.humidity = d["humidity"]
          self.samples.append(sample)
          beacons = self.args.beacons.split(',')
          for b in beacons:
            if(d["mac"] in b):
              label, macaddr = b.split("=", maxsplit = 1)
              sample.label = label
              sample.btmac = d["mac"]
              break
          logging.debug(sample.__str__())


    '''***************************************************************************

    ***************************************************************************'''
    async def publish(self):        
        if len(self.samples) == 0:
            return
        if self.firstTime == True or self.prevSamples != self.samples:
            await Publisher(self.args, self.logging).doPublish(self.samples)
            self.prevSamples = copy.deepcopy(self.samples)
            self.firstTime == False
        else:
            self.logging.debug(f"publish(): samples identical, omitting publish!")
        self.samples.clear()            
 
