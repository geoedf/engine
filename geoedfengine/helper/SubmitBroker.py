#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Provides a wrapper for HUBzero submit
"""

import sys
import os
import subprocess

class SubmitBroker:

    # method to execute a workflow using submit
    # subprocess is used to run the command-line submit tool
    # future versions will use the submit Python client
    
    def plan_and_submit(self,workflow_dir):

        # assume TC and workflow YML files are in the workflow_dir
        try:
            subprocess.run(["submit","--detach","-i","transformations.yml","pegasus-plan-geoedf","--dax","workflow.yml"],cwd=workflow_dir)
        except e:
            raise e

    # method to monitor a workflow's progress
    # for now we only return a binary, checking to see if pegasus.analysis 
    # exists in workflow_dir
    
    def monitor_status(self,workflow_dir):
        if os.path.exists('%s/pegasus.analysis' % workflow_dir):
            print("workflow is complete; check pegasus.analysis file in this directory to check success")
        else:
            print("workflow is still executing...")
