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
from .GeoEDFConfig import GeoEDFConfig

class GeoEDFWorkflow:

    # def_filename is a YAML file that encodes the workflow
    # target is determined from the config file
    def __init__(self,def_filename=None):

        # fetch the config
        self.geoedf_cfg = GeoEDFConfig()

        # if config was not set up, assume this is in submit mode
        # submit mode is used only for constructing sub-workflows on the submit node
        if self.geoedf_cfg.config is not None:
            # figure out whether prod or dev mode
            self.mode = self.geoedf_cfg.config['GENERAL']['mode']

            # figure out workflow execution target
            self.target = self.geoedf_cfg.config['GENERAL']['target']
        
            # set environment variables necessary for Singularity registry client
            # these are fetched from the config
            os.environ['SREGISTRY_CLIENT'] = self.geoedf_cfg.config['REGISTRY']['registry_client']
            os.environ['SREGISTRY_REGISTRY_BASE'] = self.geoedf_cfg.config['REGISTRY']['registry_base']
        else:
            self.mode = 'submit'
            self.target = 'condorpool'

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
        self.builder = WorkflowBuilder(def_filename,self.mode,self.target)

        # build the concrete Pegasus workflow
        self.builder.build_pegasus_dax()

        # write out final replica catalog
        self.builder.rc.write()

        # get the dax
        self.geoedf_wf = self.builder.geoedf_wf

    # executes the Pegasus DAX constructed by the builder
    def execute(self):

        # in dev mode, we execute; otherwise just write out the workflow so we can use submit
        if self.mode == 'dev':
            # set the replica catalog for this workflow
            self.geoedf_wf.add_replica_catalog(self.builder.rc)

            # prepare for outputs
            output_dir = '%s/output' % self.builder.run_dir

            # inform user
            print("On successful completion, outputs will be placed at: %s" % output_dir)
        
            # plan and execute workflow
            self.geoedf_wf.plan(dir=self.builder.run_dir,output_dir=output_dir,submit=True).wait()
        else:
            self.geoedf_wf.write()
