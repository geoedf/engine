#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Top-level workflow class; parses a YAML workflow and creates a Pegasus DAX 
    with sub-workflows for each connector, processor instance created by the 
    respective classes.
"""

import sys
import os
import yaml
import re
import importlib
import itertools
import getpass
import random
from functools import reduce

from ..helper.GeoEDFError import GeoEDFError

class GeoEDFWorkflow:
    
    # can either provide a standalone YAML file of the workflow or 
    # provide a dictionary encoding the workflow
    # file takes precedence; ignore dict if both provided
    def __init__(self,def_filename=None,def_dict=None):
        
    # parses a string to find the mentioned variables: %{var}
    def find_dependent_vars(self,value):
        if value is not None and isinstance(value, str):
            return re.findall('\%\{(.+?)\}',value)
        else:
            return []

