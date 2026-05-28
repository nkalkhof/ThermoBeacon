#!/usr/bin/env python3
'''***************************************************************************
 * 
 * -------------------------------------------------------------------------
 * begin                : 
 * last changes         : 
 * copyright            : (C) N.Kalkhof
 * email                : info@kalkhof-it-solutions.de
 ***************************************************************************'''

import math
from dataclasses import dataclass

'''***************************************************************************

***************************************************************************'''
@dataclass
class Sample:
    def __init__(self, 
                 label       = None,
                 btmac       = None,
                 temperature = math.nan, 
                 humidity    = math.nan):
        self.label:      str   = label                 
        self.btmac:      str   = btmac
        self.temperature:float = temperature
        self.humidity:   float = humidity

    '''***************************************************************************

    ***************************************************************************'''
    def __str__(self):             
        return ("\n========> sample data <========\n"
            "label:\t\t{0}\nmac:\t\t{1}\ntemperature:\t{2:3.2f}°C\nhumidity:\t{3:3.2f}%".
            format(self.label, self.btmac, self.temperature, self.humidity))


'''***************************************************************************

***************************************************************************'''
def compareSamples(samples, prevsamples) -> bool:
    if prevsamples is None or len(prevsamples) == 0: 
        return False

    if len(prevsamples) < len(samples): 
        return False

    for sample in samples:
        for prevsample in prevsamples:
            if len(sample.btmac) != 0 and sample.btmac == prevsample.btmac:
                if round(sample.temperature, 2) != round(prevsample.temperature, 2) and \
                   round(sample.humidity, 2)    != round(prevsample.humidity, 2):
                    return False
    return True
