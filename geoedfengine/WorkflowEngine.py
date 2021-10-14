#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Top-level workflow engine class: implements key methods for constructing,
    executing, and monitoring a GeoEDF workflow.
    Functions as a "controller" of sorts; will utilize the GeoEDFWorkflow class 
    as the "model" for representing workflows
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
from .helper.WorkflowUtils import WorkflowUtils
from .helper.WorkflowDBHelper import WorkflowDBHelper
from .GeoEDFConfig import GeoEDFConfig
from .GeoEDFWorkflow import GeoEDFWorkflow

# fetch the config
geoedf_cfg = GeoEDFConfig()

# validation (1) if config was not set up, assume this is in submit mode
# submit mode is used only for constructing sub-workflows on the submit node
if geoedf_cfg.config is not None:
    # figure out whether prod or dev mode
    mode = geoedf_cfg.config['GENERAL']['mode']

    # figure out workflow execution target
    target = geoedf_cfg.config['GENERAL']['target']

    # figure out the middleware(broker) being used to execute the workflow
    # on our behalf
    broker = geoedf_cfg.config['GENERAL']['broker']
        
    # set environment variables necessary for Singularity registry client
    # these are fetched from the config
    os.environ['SREGISTRY_CLIENT'] = geoedf_cfg.config['REGISTRY']['registry_client']
    os.environ['SREGISTRY_REGISTRY_BASE'] = geoedf_cfg.config['REGISTRY']['registry_base']
else:
    mode = 'submit'
    target = 'condorpool'

class WorkflowEngine:

    # determine tool_shortname (HUBzero specific)
    # specific to HUBzero: this is the tool where GeoEDF is installed
    # when monitoring, only workflows executed from this tool will be
    # retrieved
    @staticmethod
    def get_tool_shortname():
        #default value
        tool_shortname = 'geoedf'
        if os.getenv('TOOLDIR') is not None:
            tooldir = os.getenv('TOOLDIR')
            if tooldir.startswith('/apps/'):
                # assuming tooldir is of the form: /apps/tool_shortname/release/
                portions = tooldir.split('/')
                if len(portions) > 2:
                    tool_shortname = portions[2]
        return tool_shortname
            
    # workflow_file is a YAML file that encodes the workflow
    # workflow_name is optional and is used to override the default name
    # assigned by GeoEDF; can be used to monitor status later
    # once workflow is validated; it will be executed
    @staticmethod
    def execute_workflow(workflow_file,workflow_name=None):

        # get a workflow util helper
        helper = WorkflowUtils()

        # validation (0) make sure a valid workflow file has been provided
        if workflow_file is not None:
            if os.path.isfile(workflow_file):
                # initialize the workflow (and validate it)
                workflow = GeoEDFWorkflow(workflow_file,workflow_name,mode,target,WorkflowEngine.get_tool_shortname())
                print("Workflow %s created" % workflow.workflow_name)
                
                # in dev mode, we execute; otherwise just write out the workflow so we can use submit
                if mode == 'dev':
                    # set the replica catalog for this workflow
                    workflow.geoedf_wf.add_replica_catalog(workflow.builder.rc)
                    # prepare for outputs
                    output_dir = '%s/output' % workflow.workflow_rundir

                    # inform user
                    print("On successful completion, outputs will be placed at: %s" % output_dir)
        
                    # plan and execute workflow
                    workflow.geoedf_wf.plan(dir=workflow.workflow_rundir,output_dir=output_dir,submit=True).wait()
                # prod mode
                else:
                    workflow.geoedf_wf.write('%s/workflow.yml' % workflow.workflow_rundir)
                    helper.execute_workflow(workflow.workflow_rundir,broker)
                    print("Workflow submitted for execution; outputs will be written to %s" % workflow.workflow_rundir)
                    print("Workflow execution can be monitored by passing the workflow name: %s to the monitor() method" % workflow.workflow_name)
            else:
                raise GeoEDFError('A valid workflow file path needs to be provided for execution!')
        else:
            raise GeoEDFError('A workflow_file argument needs to be provided to execute!')

    # monitor workflow execution
    # if workflow_name is not provided, return status for all workflows
    # run using this tool (HUBzero specific)
    @staticmethod
    def workflow_status(workflow_name=None):
        # retrieve status by querying the workflow DBs
        workflowdb_helper = WorkflowDBHelper()

        status_res = workflowdb_helper.get_workflow_status(workflow_name,WorkflowEngine.get_tool_shortname())
        return status_res
