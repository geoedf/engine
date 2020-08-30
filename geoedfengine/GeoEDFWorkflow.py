#!/usr/bin/env python3
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

from Pegasus.api import *

from .helper.GeoEDFError import GeoEDFError
from .helper.WorkflowBuilder import WorkflowBuilder
from .helper.WorkflowUtils import WorkflowUtils
from GeoEDFConfig import GeoEDFConfig

class GeoEDFWorkflow:

    # def_filename is a YAML file that encodes the workflow
    # target corresponds to the config entry in an execution config file
    # possible values are 'local', 'geoedf-public', 'cluster#', 'condorpool' (for testing)
    def __init__(self,def_filename=None,target='condorpool'):

        # fetch the config
        self.geoedf_cfg = GeoEDFConfig()
        
        # set environment variables necessary for Singularity registry client
        # these are fetched from the config
        os.environ['SREGISTRY_CLIENT'] = self.geoedf_cfg.config['REGISTRY']['registry_client']
        os.environ['SREGISTRY_REGISTRY_BASE'] = self.geoedf_cfg.config['REGISTRY']['registry_base']

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

        # write out final replica catalog
        self.builder.rc.write()

        # get the dax
        self.geoedf_wf = self.builder.geoedf_wf

        # execution target
        self.target = target

    # executes the Pegasus DAX constructed by the builder
    def execute(self):

        # set execution properties
        props = Properties()

        # in dev mode, we don't need many of the transfer properties
        if self.geoedf_cfg.config['DEFAULT']['mode'] == 'dev':
            props['pegasus.integrity.checking'] = 'none'
        else:
            props['pegasus.integrity.checking'] = 'none'
            props['pegasus.data.configuration'] = 'nonsharedfs'
            props['pegasus.transfer.worker.package'] = 'true'
            props['pegasus.condor.arguments.quote'] = 'false'
            props['pegasus.transfer.links'] = 'true'

        # set the replica catalog for this workflow
        self.geoedf_wf.add_replica_catalog(self.builder.rc)

        # prepare for outputs
        output_dir = '%s/output' % self.builder.run_dir
        self.geoedf_wf.plan(dir=self.builder.run_dir,output_dir=output_dir,submit=True).wait()
        



        
