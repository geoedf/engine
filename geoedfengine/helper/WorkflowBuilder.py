#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Provides workflow building utilities for GeoEDFWorkflow
    Utilizes the GeoEDFConnector and GeoEDFProcessor classes to 
    derive a sub-workflow DAX corresponding to the Connector or Processor 
    plugin that gets added as a job in the main workflow
"""

import sys
import os
import yaml
import re

from GeoEDFError import GeoEDFError
from GeoEDFConnector import GeoEDFConnector
from GeoEDFProcessor import GeoEDFProcessor
from WorkflowUtils import WorkflowUtils

class WorkflowBuilder

    CONNECTOR = 1
    PROCESSOR = 2

    # initialize builder; do any necessary init tasks
    # pass along anything that may be needed; for now, just the target execution environment
    # creates local workflow directory to hold subworkflow DAX XMLs
    # and merge result outputs

    # constants for executables that create connector and processor subdax
    # need to determine where the executable is installed using "which"
    build_conn_subdax = Executable(name="build_conn_subdax", arch="x86_64", installed=False)
    conn_builder_path = WorkflowUtils.find_exec_path("build_connector_subdax")
    build_conn_subdax.addPFN(PFN("file://%s" % conn_builder_path,"local"))
    
    build_proc_subdax = Executable(name="build_proc_subdax", arch="x86_64", installed=False)
    proc_builder_path = WorkflowUtils.find_exec_path("build_processor_subdax")
    build_proc_subdax.addPFN(PFN("file://%s" % proc_builder_path,"local"))
    
    def __init__(self,workflow_dict,target='local'):
        self.workflow_dict = workflow_dict
        self.target = target

    # initialize Pegasus DAX with a few common admin tasks
    # e.g. job for creating the remote data directory for the workflow
    # also creates the local run directory for this workflow
    # returns the DAX
    def init_workflow(self):

        # determine name of new directory to hold intermediate results on target site
        self.job_dir = self.new_target_job_dir(self.target)

        # initialize DAX with job creating this directory on target site
        self.dax = <>
        
        # create a local run directory 
        self.run_dir = <>

        
    # build a Pegasus DAX using the workflow-dict
    def build_pegasus_dax(self):

        # first initialize with some admin jobs
        self.init_workflow()

        # determine number of stages & loop through them creating sub-workflow for each

        num_stages = len(self.workflow_dict)

        for curr_stage in range(1,num_stages+1):

            stage_id = '$%d' % curr_stage

            # file to hold the subdax for this stage
            stage_name = 'stage-%d' % curr_stage
            subdax_filename = '%s.xml' % stage_name

            # add subdax file to DAX
            subdax_file = File(subdax_filename)
            subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
            subdax_file.addPFN(PFN("file://%s" % subdax_filepath, "local"))
            self.dax.addFile(subdax_file)

            # build a subdax for this stage and save to subdax file
            workflow_stage  = self.workflow_dict[stage_id]

            builder.construct_stage_subdax(workflow_stage,subdax_filepath)

        # create a "local" job to construct sub-workflow for current stage (use meaningful name for xml file)
        # this needs an executable (script to call class method on connector or processor class)
        # what are the arguments to the script?


        # add job executing sub-workflow to DAX with correct job dependencies


        

    # construct subdax for a given workflow stage 
    def construct_stage_subdax(self,workflow_stage,subdax_filepath):

        # naive implementation of connector | processor check
        if 'Input' in workflow_stage.keys():
            stage_type = WorkflowBuilder.CONNECTOR
        else:
            stage_type = WorkflowBuilder.PROCESSOR

        # construct subdax and save in subdax file
        # executable to be used differs based on whether we are
        # converting a connector or processor instance
        if stage_type == WorkflowBuilder.CONNECTOR:
            subdax_build_job = Job(name="build_conn_subdax")
        elif stage_type == WorkflowBuilder.PROCESSOR:
            subdax_build_job = Job(name="build_proc_subdax")

        # always run this job locally
        subdax_build_job.addProfile(Profile("hints", "execution.site", "local"))

        # job arguments include the filepath for the subdax XML file to be created
        # job directory on target site
        # workflow YAML file
        # variable and data bindings for the current stage
        subdax_build_job.addArguments(subdax_filepath,self.job_dir)
        
        
                
        
