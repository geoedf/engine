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
from Pegasus.jupyter.instance import *

from ..helper.GeoEDFError import GeoEDFError
from ..helper.WorkflowBuilder import WorkflowBuilder

class GeoEDFWorkflow:

    # can either provide a standalone YAML file of the workflow or 
    # provide a dictionary encoding the workflow
    # file takes precedence; ignore dict if both provided
    # target corresponds to the config entry in an execution config file
    # possible values are 'local', 'geoedf-public', 'cluster#'
    def __init__(self,def_filename=None,def_dict=None,target='local'):

        # validation (1) make sure at-least one of file or dict is provided
        if def_filename is None and def_dict is None:
            raise GeoEDFError('Error: a workflow input either as a YAML file or a dictionary must be provided!')

        # create a GeoEDF workflow object from the input file
        if def_filename is not None:  # file takes precedence
            with open(def_filename,'r') as workflow_file:
                self.workflow_dict = yaml.load(workflow_file)
        else:
            self.workflow_dict = def_dict

        # syntactic and semantic validation (2) ....

        # after validation suceeds, create a builder for this workflow
        builder = WorkflowBuilder(self.workflow_dict,target)

        # build the concrete Pegasus workflow
        self.dax = builder.build_pegasus_dax()

    # executes the Pegasus DAX constructed by the builder
    def execute(self):

        # write DAX out to file and submit or use Jupyter API


        
