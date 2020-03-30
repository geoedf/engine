#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Top-level workflow class: accepts a GeoEDF workflow encoded as YAML file or dictionary. 
    Validates the GeoEDF workflow before conversion to Pegasus DAX
    Utilizes a WorkflowBuilder to convert YAML workflow to Pegasus DAX.
    This class is responsible for executing and monitoring the workflow. 
"""

import sys
import os
import yaml
import re
import itertools
import random
from functools import reduce

from Pegasus.DAX3 import *
from Pegasus.jupyter.instance import *

from .helper.GeoEDFError import GeoEDFError
from .helper.WorkflowBuilder import WorkflowBuilder

class GeoEDFWorkflow:

    # def_filename is a YAML file that encodes the workflow
    # target corresponds to the config entry in an execution config file
    # possible values are 'local', 'geoedf-public', 'cluster#', 'condorpool' (for testing)
    def __init__(self,def_filename=None,target='condorpool'):

        # validation (1) make sure workflow file has been provided
        if def_filename is None:
            raise GeoEDFError('Error: a workflow YAML file must be provided!')

        # create a GeoEDF workflow object from the input file
        with open(def_filename,'r') as workflow_file:
            self.workflow_dict = yaml.load(workflow_file)

        # syntactic and semantic validation (2) ....

        # after validation suceeds, create a builder for this workflow
        self.builder = WorkflowBuilder(def_filename,target)

        # build the concrete Pegasus workflow
        self.builder.build_pegasus_dax()

        # get the dax
        self.dax = self.builder.dax

        # execution target
        self.target = target

    # executes the Pegasus DAX constructed by the builder
    def execute(self):
        # get a workflow instance to execute
        self.instance = self.builder.get_workflow_instance()
        self.instance.run(site=self.target)
        self.status(loop=True)



        
