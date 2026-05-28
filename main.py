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
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler

from thermobeacon.args import getArgs
from thermobeacon.controller import Controller

'''***************************************************************************

***************************************************************************'''
def setupLogging(args, name):
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    fileHandler = TimedRotatingFileHandler(args.logpath, 
            when = 'h', interval = 24, backupCount = 7)
    fileHandler.setFormatter(formatter)    
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    logger = logging.getLogger(name)
    if args.logger == "file":
        logger.addHandler(fileHandler)
    else:
        logger.addHandler(consoleHandler)    
    logger.setLevel(args.loglevel.upper())
    logger.propagate = False # keep logs to this module!
    # shut up bleak debug logging flood
    bleak_logger = logging.getLogger("bleak")
    bleak_logger.setLevel(logging.INFO)
    return logger


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
    args = getArgs()     
    setupLogging(args, None)             
    controller: Controller = Controller(args = args, logger = logging.getLogger())    
    controller.connect()
    while(True):
        starttime = time.time()
        await controller.scan()
        controller.publish()
        delta = args.interval - (time.time() - starttime)
        if delta > 0:
            logging.debug(f"sleeping for: {delta} seconds...")    
            await asyncio.sleep(delta)
        
if __name__ == "__main__":
    asyncio.run(main())

