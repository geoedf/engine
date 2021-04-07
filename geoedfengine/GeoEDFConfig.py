#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Simple configuration class to customize a GeoEDF engine deployment
    Configuration options set in a geoedf.cfg file are parsed using ConfigParser    
    The geoedf.cfg file is fetched from the standard path /usr/local/config or 
    wherever the environment variable GEOEDF_CONFIG says it is present
    if not found then assume "submit" mode where it is only used for constructing subworkflows
"""

import sys
import os
import configparser
from .helper.GeoEDFError import GeoEDFError

class GeoEDFConfig:

    def __init__(self):
        # creates a config object based on parsing config file
        # environment variable can be used to override standard config file path
        # if not found, assume in "submit" mode

        config_filepath = str(os.getenv('GEOEDF_CONFIG','/usr/local/config/geoedf.cfg'))

        if not (os.path.exists(config_filepath) and os.path.isfile(config_filepath)):
            self.config = None
            print("Could not find GeoEDF config file; unless this is the submit host, workflows will fail!")
        else:
            # parse config
            self.config = configparser.ConfigParser()
            self.config.read(config_filepath)
