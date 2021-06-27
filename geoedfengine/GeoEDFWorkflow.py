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
from .helper.WorkflowMonitor import WorkflowMonitor
from .GeoEDFConfig import GeoEDFConfig

class GeoEDFWorkflow:

    # def_filename is a YAML file that encodes the workflow
    # workflow_dir is the directory corresponding to a previously created workflow
    # target is determined from the config file
    # workflow_dir is used to instantiate a prior workflow and monitor it
    # either one of def_filename or workflow_dir is required
    # def_filename takes precedence
    def __init__(self,def_filename=None,workflow_name=None):

        # validation (0) make sure workflow file has been provided
        # if not, check if workflow_name is provided, in this case we simply monitor
        if def_filename is None:
            if workflow_name is None:
                raise GeoEDFError('Error: a workflow YAML file or a submitted workflow name must be provided!')
            else:
                self.workflow_name = workflow_name
                self.mode = 'monitor'
                self.target = None
                # short circuit
                return

        # fetch the config
        self.geoedf_cfg = GeoEDFConfig()

        # validation (1) if config was not set up, assume this is in submit mode
        # submit mode is used only for constructing sub-workflows on the submit node
        if self.geoedf_cfg.config is not None:
            # figure out whether prod or dev mode
            self.mode = self.geoedf_cfg.config['GENERAL']['mode']

            # figure out workflow execution target
            self.target = self.geoedf_cfg.config['GENERAL']['target']

            # figure out the middleware(broker) being used to execute the workflow
            # on our behalf
            self.broker = self.geoedf_cfg.config['GENERAL']['broker']
        
            # set environment variables necessary for Singularity registry client
            # these are fetched from the config
            os.environ['SREGISTRY_CLIENT'] = self.geoedf_cfg.config['REGISTRY']['registry_client']
            os.environ['SREGISTRY_REGISTRY_BASE'] = self.geoedf_cfg.config['REGISTRY']['registry_base']
        else:
            self.mode = 'submit'
            self.target = 'condorpool'

        # get a helper; this happens first since we need it for creation, execution, and monitoring
        self.helper = WorkflowUtils()

        # create a GeoEDF workflow object from the input file
        with open(def_filename,'r') as workflow_file:
            self.workflow_dict = yaml.load(workflow_file,Loader=FullLoader)

        # validate this workflow
        self.helper.validate_workflow(self.workflow_dict)

        # after validation suceeds, create a builder for this workflow
        self.builder = WorkflowBuilder(def_filename,self.mode,self.target)

        # build the concrete Pegasus workflow
        self.builder.build_pegasus_dax()

        # write out final replica catalog (see note in WorkflowBuilder about not needing RC)
        # self.builder.rc.write()

        # get the dax
        self.geoedf_wf = self.builder.geoedf_wf

    # executes the Pegasus DAX constructed by the builder
    def execute(self):

        # in monitor mode, we do not allow execution; print message and return
        if self.mode == 'monitor':
            print("This workflow has already been submitted for execution; use the monitor() method instead.")
            return

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
            self.geoedf_wf.write('%s/workflow.yml' % self.builder.run_dir)
            print("Workflow %s created" % self.geoedf_wf.name)
            self.helper.execute_workflow(self.builder.run_dir,self.broker)
            print("Workflow submitted for execution; outputs will be written to %s" % self.builder.run_dir)
            # initialize workflow monitoring
            self.workflow_monitor = WorkflowMonitor()
            # start monitoring this new workflow
            self.workflow_monitor.start_monitor(self.geoedf_wf.name)
            print("Workflow execution can be monitored using the workflow name %s and the monitor() method" % self.geoedf_wf.name)

    # monitor workflow execution
    def monitor(self):
        # use the new workflow monitor class to monitor workflow execution
        # by querying the workflow DBs
        self.workflow_monitor = WorkflowMonitor()
        monitor_res = self.workflow_monitor.monitor_status(self.workflow_name)
        print(monitor_res)
