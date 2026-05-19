#!/usr/bin/env python3
'''***************************************************************************
 * @see: https://novelbits.io/bluetooth-low-energy-advertisements-part-1/
 * -------------------------------------------------------------------------
 * begin                : Sept 01 2022
 * last changes         : Sept 29 2023
 * copyright            : (C) 2022,2023 by N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 **************************************************************************'''
import signal
import time
import math
import asyncio
import time
import logging
from logging.handlers import TimedRotatingFileHandler

from controller import Controller
from args import getArgs

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


'''***************************************************************************

***************************************************************************'''
async def main():    
    __setupLogging()             
    controller: Controller = Controller(args = args, logger = logging.getLogger())    
    while(True):
        await controller.connect()
        await controller.read()
        await controller.publish()
        
asyncio.run(main())

