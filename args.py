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

'''***************************************************************************
# assemble arguments
***************************************************************************'''
def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument( "--dbhost",
                        help="database hostname",
                        default="localhost",
                        type=str, required=False)
    parser.add_argument( "--ilpport",
                        help="database ilp port",
                        default="9000",
                        type=str, required=False)
    parser.add_argument("--dbtable",
                        help="database table",
                        default="thermobeacon",
                        type=str, required=False)
    parser.add_argument("-b", "--beacons",
                        #"--beacons", "living=6f:15:00:00:00:42,bed=6f:15:00:00:0c:b1,bath=8e:72:00:00:03:25,fridge=23:1b:00:00:04:76,outside=8e:d6:00:00:06:ca",
                        help="beacons mac,label",
                        type=str, required=True)
    parser.add_argument("--interval",
                        help="sample interval",
                        default = 30,
                        type=int, required=False)
    parser.add_argument("--scantimeout",
                        help="BLE scantimeout",
                        default = 10,
                        type=int, required=False)
    parser.add_argument("--logger",
                        help="log output file|console",
                        default="console",
                        type=str, required=False)
    parser.add_argument("--loglevel",
                        help="debug|info",
                        default="info",
                        type=str, required=False)
    parser.add_argument("--logpath",
                        help="logpath",
                        default="/tmp/thermobeacon.log",
                        type=str, required=False)

    return parser.parse_args()
