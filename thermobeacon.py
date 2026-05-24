#!/usr/bin/env python3
'''***************************************************************************
 * @desc:
 * -------------------------------------------------------------------------
 * begin                : Sept 01 2022
 * last changes         : May 20 2026
 * copyright            : (C) 2022,2026 by N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 **************************************************************************'''
import signal
import sys
import time
import math
import asyncio
import time
import logging
from logging.handlers import TimedRotatingFileHandler

from Thermobeacon.controller import Controller
from Thermobeacon.args import getArgs

args = getArgs()

'''***************************************************************************

***************************************************************************'''
def __setupLogging():
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    fileHandler = TimedRotatingFileHandler(args.logpath, 
            when = 'h', interval = 24, backupCount = 7)
    fileHandler.setFormatter(formatter)    
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger = logging.getLogger()
    if args.logger == "file":
        logger.addHandler(fileHandler)
    else:
        logger.addHandler(consoleHandler)    
    logger.setLevel(args.loglevel.upper())
    # shut up bleak debug logging flood
    bleak_logger = logging.getLogger("bleak")
    bleak_logger.setLevel(logging.INFO)


def signal_handler(signum, frame):
    logging.info('signal received, trying to close connections...')
    logging.info('shutting down logging...')
    logging.shutdown()    
    sys.exit(0)
    
def exception_handler(type, value, tb):
    logging.info("Uncaught exception: {0}".format(str(value)))

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGHUP, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)

'''***************************************************************************

***************************************************************************'''
async def main():    
    __setupLogging()             
    controller: Controller = Controller(args = args, logger = logging.getLogger())    
    await controller.connect()
    while(True):
        starttime = time.time()
        await controller.scan()
        await controller.publish()
        delta = args.interval - (time.time() - starttime)
        if delta > 0:
            logging.debug(f"sleeping for: {delta} seconds...")    
            await asyncio.sleep(delta)
        
asyncio.run(main())

