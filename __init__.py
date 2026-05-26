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

from .controller import Controller as Thermo
from .args import getArgs as Thermoargs

__all__ = ['getArgs', 'runExternal']
