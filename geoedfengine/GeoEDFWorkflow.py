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
from yaml import FullLoader
import re
import itertools
import random
from functools import reduce

from Pegasus.DAX3 import *
from Pegasus.jupyter.instance import *

from .helper.GeoEDFError import GeoEDFError
from .helper.WorkflowBuilder import WorkflowBuilder
from .helper.WorkflowUtils import WorkflowUtils

class GeoEDFWorkflow:

    # def_filename is a YAML file that encodes the workflow
    # target corresponds to the config entry in an execution config file
    # possible values are 'local', 'geoedf-public', 'cluster#', 'condorpool' (for testing)
    def __init__(self,def_filename=None,target='condorpool'):

        # set environment variables necessary for Singularity registry client
        os.environ['SREGISTRY_CLIENT'] = 'registry'
        os.environ['SREGISTRY_REGISTRY_BASE'] = 'https://www.registry.geoedf.org'

        # validation (1) make sure workflow file has been provided
        if def_filename is None:
            raise GeoEDFError('Error: a workflow YAML file must be provided!')

        # create a GeoEDF workflow object from the input file
        with open(def_filename,'r') as workflow_file:
            self.workflow_dict = yaml.load(workflow_file,Loader=FullLoader)

        # syntactic validation ....
        self.helper = WorkflowUtils()

        # validate this workflow
        self.helper.validate_workflow(self.workflow_dict)

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
        # turn off integrity checking (for now)
        # TODO: figure out why Pegasus is not returning checksum in metadata
        # causes integrity checking to fail
        self.instance.set_property('pegasus.integrity.checking','none')
        self.instance.set_property('pegasus.data.configuration','nonsharedfs')
        self.instance.set_property('pegasus.transfer.worker.package','true')
        self.instance.set_property('pegasus.condor.arguments.quote','false')
        self.instance.set_property('pegasus.transfer.links','true')
        self.instance.run(site=self.target)
        self.instance.status(loop=True)



        
