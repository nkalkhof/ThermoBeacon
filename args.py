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
import argparse
import configparser
from pathlib import Path

DEFAULT_CONFIGFILE_NAME = "config.ini"
DEFAULT_SECTION_NAME = "thermobeacon"

'''***************************************************************************
# assemble arguments
***************************************************************************'''
def getArgs():
    packagepath = Path(__file__).resolve().parent
    config = configparser.ConfigParser()
    config.read(packagepath / DEFAULT_CONFIGFILE_NAME)
    
    dbhost   = config.get(DEFAULT_SECTION_NAME,    "dbhost",   fallback = "localhost")
    ilpport  = config.getint(DEFAULT_SECTION_NAME, "ilpport",  fallback = 9000)
    dbtable  = config.get(DEFAULT_SECTION_NAME,    "dbtable",  fallback = "thermobeacon")
    beacons  = config.get(DEFAULT_SECTION_NAME,    "beacons",  fallback = "living=6f:15:00:00:00:42,bed=6f:15:00:00:0c:b1,bath=8e:72:00:00:03:25,fridge=23:1b:00:00:04:76,outside=8e:d6:00:00:06:ca")
    interval = config.getint(DEFAULT_SECTION_NAME, "interval", fallback = 5)
    scantime = config.getint(DEFAULT_SECTION_NAME, "scantime", fallback = 10)
    logger   = config.get(DEFAULT_SECTION_NAME,    "logger",   fallback = "console")
    loglevel = config.get(DEFAULT_SECTION_NAME,    "loglevel", fallback = "info")
    logpath  = config.get(DEFAULT_SECTION_NAME,    "logpath",  fallback = "/tmp/thermobeacon.log")

    parser = argparse.ArgumentParser()
    parser.add_argument("--dbhost",
                        help="database hostname",
                        default=dbhost,
                        type=str, required=False)
    parser.add_argument("--ilpport",
                        help="database ilp port",
                        default=ilpport,
                        type=str, required=False)
    parser.add_argument("--dbtable",
                        help="database table",
                        default=dbtable,
                        type=str, required=False)
    parser.add_argument("--beacons",
                        help="mac address",
                        default = beacons,
                        type=str, required=False)
    parser.add_argument("--interval",
                        help="sample interval",
                        default = interval,
                        type=int, required=False)
    parser.add_argument("--scantime",
                        help="BLE scantime",
                        default = scantime,
                        type=int, required=False)
    parser.add_argument("--logger",
                        help="log output file|console",
                        default=logger,
                        type=str, required=False)
    parser.add_argument("--loglevel",
                        help="debug|info",
                        default=loglevel,
                        type=str, required=False)
    parser.add_argument("--logpath",
                        help="logpath",
                        default=logpath,
                        type=str, required=False)

    return parser.parse_args()
