#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Simple configuration class to customize a GeoEDF engine deployment
    Configuration options set in a geoedf.cfg file are parsed using ConfigParser    
    The geoedf.cfg file is fetched from a prioritized set of locations as follows:
    1. user home .config directory
    2. Conda local/config directory
    3. System /usr/local/config directory
"""

import sys
import os
import configparser

class GeoEDFConfig:

    def __init__(self):
        # fetches config file from the prioritized list of paths,
        # creates a config object based on parsing

        home_dir = os.getenv('HOME')

        conda_dir = '/opt/conda'

        std_dir = '/usr/local/config'

        home_dir_config = '%s/.config/geoedf.cfg' % home_dir
        conda_dir_config = '%s/usr/local/config/geoedf.cfg' % conda_dir
        std_dir_config = '%s/geoedf.cfg' % std_dir

        if os.path.exists(home_dir_config) and os.path.isfile(home_dir_config):
            config_file = home_dir_config
        elif os.path.exists(conda_dir_config) and os.path.isfile(conda_dir_config):
            config_file = conda_dir_config
        elif os.path.exists(std_dir_config) and os.path.isfile(std_dir_config):
            config_file = std_dir_config

        # parse config
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
