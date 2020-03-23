#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Provides low-level utilities used by the WorkflowBuilder class
    Also provides a wrapper for Pegasus commands that need to be 
    run in a shell
"""

import sys
import os
import re
import subprocess

from GeoEDFError import GeoEDFError

class WorkflowUtils

    # finds the path to an executable by running "which"
    def find_exec_path(exec_name):
        try:
            which_proc = subprocess.run(["which",exec_name],stdout=subprocess.PIPE, check=True, encoding="utf-8")
            return which_proc.stdout.rstrip()
        except subprocess.CalledProcessError:
            raise GeoEDFError("Error occurred in finding the executable %s. This should not happen if the workflow engine was successfully installed!!!" % exec_name)
        

    # parses a string to find the mentioned variables: %{var}
    def find_dependent_vars(value):
        if value is not None and isinstance(value, str):
            return re.findall('\%\{(.+?)\}',value)
        else:
            return []


