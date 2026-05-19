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
            "label:\t{0}\nmac:\t{1}\ntemperature:\t{2:3.2f}°C\nhumidity:\t{3:3.2f}%".
            format(self.label, self.btmac, self.temperature, self.humidity))
