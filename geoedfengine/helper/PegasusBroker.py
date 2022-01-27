#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Provides a wrapper for Pegasus APIs
"""

import sys
import os
import subprocess

class PegasusBroker:

    # method to execute a workflow using pegasus plan
    # pegasus Python API is used here
    
    def plan_and_submit(self,workflow_dir):

        output_dir = '%s/output' % workflow_dir
        
        try:
            # assume TC and workflow YML files are in the workflow_dir
            subprocess.run(["pegasus-plan","--output-dir",output_dir,"--submit","workflow.yml"],cwd=workflow_dir)
        except e:
            raise e