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
import signal
import logging
import asyncpg
from questdb.ingress import Sender, TimestampNanos, IngressError
from Thermobeacon.sample import Sample

class Publisher():
    
    '''***************************************************************************

    ***************************************************************************'''
    def __init__(self, args, logger):
        self.logging = logger
        self.args    = args

    '''***************************************************************************

    ***************************************************************************'''
    async def doPublish(self, samples):
        try:
            rows: Dict = {}
            for sample in samples:
                rows[sample.label + "_temp"] = sample.temperature
                rows[sample.label + "_hum"] = sample.humidity

            url = "{0}:{1}".format(self.args.dbhost, self.args.ilpport)

            self.logging.debug(f"doPublish(): publishing sample to {
                url} table {self.args.dbtable}...")               

            with Sender.from_conf("http::addr=" + url + ";") as sender:
                sender.row(self.args.dbtable, columns = rows, at = TimestampNanos.now())
                sender.flush()
                
        except IngressError as e:
            self.logging.error(f"doPublish(): Failed to connect or send data: {e}")
            pass

        except Exception as e:
            self.logging.error(f"doPublish(): unexpected error: {e}")
            pass