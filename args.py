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
    parser.add_argument("-d", "--beacons",
                        help="beacons mac,label",
                        type=str, required=True)
    parser.add_argument("--interval",
                        help="sample interval",
                        default = 20,
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
