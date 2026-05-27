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
from questdb.ingress import Sender, TimestampNanos, IngressError
from sample import Sample

class Publisher():
    
    '''***************************************************************************

    ***************************************************************************'''
    def __init__(self, args, logger):
        self.logging = logger
        self.args    = args

    '''***************************************************************************

    ***************************************************************************'''
    def doPublish(self, samples):
        try:
            url = "{0}:{1}".format(self.args.dbhost, self.args.ilpport)
            self.logging.debug(f"doPublish(): publishing samples to {
                url} table {self.args.dbtable}...")               
            now = TimestampNanos.now()
            with Sender.from_conf("http::addr=" + url + ";") as sender:
                for sample in samples:
                    sender.row(self.args.dbtable, 
                        columns = {
                            'label' : sample.label,
                            'temperature' : sample.temperature,
                            'humidity'    : sample.humidity
                            }, 
                        at = now)       
                sender.flush()
                
        except IngressError as e:
            self.logging.error(f"doPublish(): Failed to connect or send data: {e}")
            pass

        except Exception as e:
            self.logging.error(f"doPublish(): unexpected error: {e}")
            pass