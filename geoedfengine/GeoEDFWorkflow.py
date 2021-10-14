#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Top-level GeoEDF workflow class: accepts a workflow encoded as a YAML file
    and an optional workflow name.
    Validates the GeoEDF workflow before conversion to Pegasus DAX
    Utilizes a WorkflowBuilder to convert YAML workflow to Pegasus DAX.
    Resulting workflow object can be used by the WorkflowEngine for execution 
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
from .helper.WorkflowDBHelper import WorkflowDBHelper

class GeoEDFWorkflow:

    # workflow_filepath is a YAML file that encodes the workflow and is required
    # workflow_name is the name of the workflow
    # execution mode is dev or prod
    # execution target is the HPC server where workflow is executed
    # tool_shortname is HUBzero specific, a means of filtering workflows
    # run from various HUBzero tools
    # if workflow_name is also provided, the user intends to override
    # the automatic name assigned to the workflow with the provided one
    # validation has already been performed by the WorkflowEngine
    def __init__(self,workflow_filepath,workflow_name=None,exec_mode='dev',exec_target='local',tool_shortname='geoedf'):

        db_helper = WorkflowDBHelper()

        # first validate to make sure workflow_name is unique
        if workflow_name is not None:     
            db_helper.check_unique_workflow(workflow_name)
            
        # get a helper; this happens first since we need it for creation, execution, and monitoring
        self.helper = WorkflowUtils()

        # create a GeoEDF workflow object from the input file
        with open(workflow_filepath,'r') as workflow_file:
            self.workflow_dict = yaml.load(workflow_file,Loader=FullLoader)

        # validate this workflow
        self.helper.validate_workflow(self.workflow_dict)

        # after validation suceeds, create a builder for this workflow
        builder = WorkflowBuilder(workflow_filepath,exec_mode,exec_target)

        # build the concrete Pegasus workflow
        builder.build_pegasus_dax()

        # get the dax
        self.geoedf_wf = builder.geoedf_wf
        self.pegasus_workflow_name = self.geoedf_wf.name

        # if workflow_name is None, use the Pegasus provided name
        if workflow_name is None:
            self.workflow_name = self.pegasus_workflow_name
        else:
            self.workflow_name = workflow_name

        # set the workflow_rundir
        self.workflow_rundir = builder.run_dir

        # insert record into workflow database
        db_helper.insert_workflow(self.workflow_name,self.pegasus_workflow_name,self.workflow_rundir,tool_shortname)
