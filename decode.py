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

from Thermobeacon.sample import Sample

'''***************************************************************************
ADVERTISING MESSAGES
Decode Manufacturer specific data from BLE Advertising message
Message length: 18 bytes
bytes | content
========================================================
00-01 | code
02-02 | 00 ?
03-03 | 0x80 if Button is pressed else 00?
04-08 | mac address
08-10 | battery level: seems that 3400 = 100% (3400 mV, not quite sure)
10-12 | temperature
12-14 | humidity
14-18 | uptime: seconds since the last reset
***************************************************************************'''
def __decode(b: bytes) -> float:
    result = int.from_bytes(b, byteorder='little')/16.0
    if result > 4000:
        result -= 4096
    return result


'''***************************************************************************

***************************************************************************'''
def decodeAdData(mac, key, bvalue) -> dict:
    if key not in [0x10, 0x11]:
        raise ValueError()
    parsed = {
        "temperature": __decode(bvalue[10:12]),
        "humidity":    __decode(bvalue[12:14])
        }       
    return parsed
        
