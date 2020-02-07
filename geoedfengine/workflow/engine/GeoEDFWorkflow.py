#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Top-level workflow class; parses a YAML workflow, creates a Pegasus DAX and 
    executes it. Each connector or processor instance in the YAML workflow is 
    converted into a dynamic Pegasus sub-workflow by the respective classes
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
from ..helper.WorkflowHelper import WorkflowHelper

class GeoEDFWorkflow:
    
    # can either provide a standalone YAML file of the workflow or 
    # provide a dictionary encoding the workflow
    # file takes precedence; ignore dict if both provided
    # target corresponds to the config entry in an execution config file
    # possible values are 'local', 'geoedf-public', 'cluster#'
    def __init__(self,def_filename=None,def_dict=None,target='local'):

        # create a helper for this workflow
        helper = WorkflowHelper(target)

        # initialize Pegasus DAX with a few common admin tasks
        self.dax = helper.init_dax()
        
        
    # executes a workflow by creating a Pegasus DAX
    def execute(self):

        # determine number of stages & loop through them creating sub-workflow for each


        # is current stage a connector or processor?


        # run a "local" job to create sub-workflow for current stage (use meaningful name for xml file)
        # this needs an executable (script to call class method on connector or processor class)


        # set up DAX with right job ordering from prior stage to current

        
