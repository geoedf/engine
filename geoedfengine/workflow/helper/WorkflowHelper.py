#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Provides common utilities required by the workflow engine
    Utilizes the GeoEDFConnector and GeoEDFProcessor classes to 
    derive each plugin instance that needs to be added as a job
    in the connector or processor sub-workflow
"""

import sys
import os
import yaml
import re

from GeoEDFError import GeoEDFError
from ..engine.GeoEDFConnector import GeoEDFConnector
from ..engine.GeoEDFProcessor import GeoEDFProcessor

class WorkflowHelper

    # initialize helper; do any necessary init tasks
    # pass along anything that may be needed; for now, just the target execution environment
    # creates local workflow directory to hold subworkflow DAX XMLs
    # and merge result outputs
    def __init__(self,target='local'):

    # initialize Pegasus DAX with a few common admin tasks
    # e.g. job for creating the remote data directory for the workflow
    # returns the DAX
    def init_dax(self):

        self.dax = <>

    # parses a string to find the mentioned variables: %{var}
    def find_dependent_vars(self,value):
        if value is not None and isinstance(value, str):
            return re.findall('\%\{(.+?)\}',value)
        else:
            return []

