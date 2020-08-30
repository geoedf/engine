#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Top-level processor class; implements the methods for parsing a processor 
    instance YAML, validating it, and deriving stage references and local file references
"""

import sys
import os
import yaml
import re
import importlib
import getpass
import random
from functools import reduce

from .GeoEDFError import GeoEDFError
from .WorkflowUtils import WorkflowUtils

class GeoEDFProcessor:
    
    # takes a dictionary that is a processor piece of a workflow
    # assume def_dict is not None; has been checked before invoking
    # workflow_stage is the index of this plugin in the workflow; used to validate stage references
    def __init__(self,def_dict,workflow_stage):

        # get a helper
        self.helper = WorkflowUtils()

        # set the definition dictionary
        self.__def_dict = def_dict
        
        # validate the definition
        # will raise an exception if this fails
        if self.validate_definition():
            # now determine the prior workflow stage references and args bound to local files
            self.stage_refs = self.helper.collect_stage_refs(self.__def_dict)
            # validate stage references
            self.validate_stage_refs(workflow_stage)
            self.local_file_args = self.helper.collect_local_file_bindings(self.__def_dict)
            self.sensitive_args = self.helper.collect_empty_bindings(self.__def_dict)
            self.dir_modified_refs = self.helper.collect_dir_modified_refs(self.__def_dict)
        else:
            raise GeoEDFError('Processor fails validation!')

    # validates a plugin's definition dictionary, making sure params are bound just 
    # once and have a binding if included; no variables are present in bindings
    # if a binding references a stage then it can only have zero or more dir modifiers in its value
    def validate_definition(self):
        # first check to make sure only one processor exists in this stage
        #PYTHON2=>3
        #proc_names = self.__def_dict.keys()
        proc_names = list(self.__def_dict.keys())
        if len(proc_names) > 1:
            raise GeoEDFError("Exactly one processor can make up a workflow stage")

        # set actual processor definition
        self.plugin_name = proc_names[0] # this is needed to determine workflow executable names
        self.proc_def = self.__def_dict[self.plugin_name]
        
        plugin_params = self.proc_def.keys()
        
        # check that no param is bound more than once
        if len(plugin_params) != len(list(set(plugin_params))):
            raise GeoEDFError('Parameters can only be bound once in a plugin')
        for plugin_param in plugin_params:
            param_val = self.proc_def[plugin_param]
            if param_val is None:
                raise GeoEDFError('Parameter must have a binding if included in definition: %s' % plugin_param)
            # disallow variables in the binding
            param_vars = self.helper.find_dependent_vars(param_val)
            if len(param_vars) > 0:
                raise GeoEDFError('Variables not allowed in processors')
            # if param val has a stage reference, it must be exactly one stage and
            # have zero or more dir modifiers applied to it
            self.helper.validate_stage_refs(param_val)
            
        return True

    #validate stage references ensuring that they are earlier than current workflow stage
    def validate_stage_refs(self,workflow_stage):
        for stage_ref in self.stage_refs:
            stage_ref_num = int(stage_ref)
            if not stage_ref_num < workflow_stage:
                raise GeoEDFError('Invalid stage reference $%d in workflow stage $%d' % (stage_ref_num,workflow_stage))

            
