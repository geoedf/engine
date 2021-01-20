#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Simple configuration class to customize a GeoEDF engine deployment
    Configuration options set in a geoedf.cfg file are parsed using ConfigParser    
    The geoedf.cfg file is fetched from the standard path /usr/local/config
"""

import sys
import os
import configparser
from .helper.GeoEDFError import GeoEDFError

class GeoEDFConfig:

    def __init__(self):
        # creates a config object based on parsing config file

        std_dir = '/usr/local/config'
        std_dir_config = '%s/geoedf.cfg' % std_dir

        if not (os.path.exists(std_dir_config) and os.path.isfile(std_dir_config)):
            raise GeoEDFError('Error: required GeoEDF config file not found in /usr/local/config')
	    
        # parse config
        self.config = configparser.ConfigParser()
        self.config.read(std_dir_config)
