#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Top-level connector class; implements the methods for parsing a connector 
    instance YAML, validating it, and deriving the dependency graph
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

class GeoEDFConnector:
    
    # takes a dictionary that is a connector piece of a workflow
    # assume def_dict is not None; has been checked before invoking
    # workflow_stage is an integer corresponding to the numerical index of
    # this plugin in the workflow; used to validate stage references
    def __init__(self,def_dict,workflow_stage):

        # get a helper
        self.helper = WorkflowUtils()

        # set the definition dictionary
        self.__def_dict = def_dict
        
        # validate the definition
        # will raise an exception if this fails
        if self.validate_definition():
            # initialize a few dictionaries to hold the dependencies for a plugin & filter bindings
            self.var_filter = dict()
            self.var_dependencies = dict()
            self.plugin_dependencies = dict()
            self.stage_refs = dict()
            self.plugin_names = dict()
            self.local_file_args = dict()
            self.sensitive_args = dict()
            self.dir_modified_refs = dict()
            # now determine the plugin dependencies (variables and stages)
            # used to drive execution order and construct bindings
            self.identify_plugin_dependencies()

            # validate the stage references that have been extracted
            self.validate_stage_refs(workflow_stage)

            # now determine args bound to local files
            # these files needs to be transferred as inputs
            # args need to be "rebound" to actual filepath on execution host
            self.identify_local_file_args()

            # determine "sensitive" args, these have no binding
            # user needs to be prompted to provide values for these args
            # arg value needs to be encrypted before sending to the execution host
            self.identify_sensitive_args()

            # determine args that have dir modifiers applied to their values
            # when executing the plugin, only one binding needs to be provided for these args
            self.identify_dir_modified_args()
        else:
            raise GeoEDFError('Connector fails validation!')

    # validates a plugin's definition dictionary, making sure params are bound just 
    # once and have a binding if included; variables are not also bound params; 
    # and returns an updated list of unbound variables. 
    # Variables are disallowed in post filters or outputs
    # section name is used to provide context for error messages 
    def validate_plugin_def(self,def_dict,bound_params,unbound_vars,section):
        plugin_params = def_dict.keys()
        # check that no param is bound more than once
        if len(plugin_params) != len(list(set(plugin_params))):
            raise GeoEDFError('Parameters can only be bound once in a plugin')
        for plugin_param in plugin_params:
            param_val = def_dict[plugin_param]
            if param_val is None:
                # special handling of password parameters in Input plugins
                # these don't need to have a value; user will be prompted for them
                if section != 'Input' or plugin_param != 'password':
                    raise GeoEDFError('Parameter must have a binding if included in definition: %s' % plugin_param)
            # process variables in the binding
            param_vars = self.helper.find_dependent_vars(param_val)
            if len(param_vars) > 0 and section == 'Output':
                raise GeoEDFError('Variables not allowed in output plugins')
            else:
                for var in self.helper.find_dependent_vars(param_val):
                    if var in unbound_vars:
                        raise GeoEDFError('Cannot reuse variable: %s' % var)
                    unbound_vars.append(var)
            # validate any stage references in param value
            # will raise exception if any error
            self.helper.validate_stage_refs(param_val)
                
        # check that variables are not also bound parameters
        # have to use names distinct from 'reserved' plugin parameter names
        # first update the set of bound params
        bound_params += plugin_params
        if set(bound_params).intersection(set(unbound_vars)):
            raise GeoEDFError('A variable cannot also be a bound plugin parameter')

        return [bound_params,unbound_vars]
            
        
    # validates the connector definition by making sure that parameters are bound just once,
    # variables are not reused, and that each variable is bound
    # output plugin cannot contain any variables
    def validate_params(self):
        # keep a list of vars unbound so far; any new binding has to be of one of these vars
        # since var names cannot be reused across plugins to avoid confusion, one list is sufficient
        # also keep a list of bound vars to check that all variables have been bound at the end
        unbound_vars = []
        bound_vars = []
        bound_params = []
        
        try:

            # each plugin type has its own structure, needs special processing
            # first process the Input plugin
            section = 'Input'
            for input_plugin in self.__def_dict[section]:
                [bound_params,unbound_vars] = self.validate_plugin_def(self.__def_dict[section][input_plugin], \
                                                                       bound_params,unbound_vars,'Input')
            # then the Filter (if it exists)
            section = 'Filter'
            # some of these params can be input params
            if section in self.__def_dict:
                for filtered_param in self.__def_dict[section]:
                    # if an input param bound by a filter was already bound in the input definition, then raise error
                    #if filtered_param not in unbound_vars:
                    #    raise GeoEDFError('Only variables can be bound by a filter: %s' % filtered_param)
                    #elif filtered_param in bound_vars:
                    if filtered_param in bound_vars:
                        raise GeoEDFError('A variable can only be bound once by a filter: %s' % filtered_param)
                    else: # add this to the set of bound variables
                        bound_vars.append(filtered_param)
                            
                    # get this parameter's filter definition
                    for param_pre_filter in self.__def_dict[section][filtered_param]:
                        [bound_params,unbound_vars] = self.validate_plugin_def( \
                                                            self.__def_dict[section][filtered_param][param_pre_filter], \
                                                            bound_params,unbound_vars,'Filter')

            # make sure all variables have been bound
            if len(bound_vars) != len(unbound_vars):
                raise GeoEDFError('All variables need to be bound by filters')
            else:
               for var in unbound_vars:
                   if var not in bound_vars:
                       raise GeoEDFError('Variable %s does not have a binding' % var)
            return True

        except GeoEDFError:
            raise

    # validate connector definition to ensure that the right number of plugins are specified,
    # and that there are no circular or improper dependencies between variables
    def validate_definition(self):
        # first check to make sure it has the required Input section
        if 'Input' in self.__def_dict:
            # make sure it has just one Input plugin class
            if len(self.__def_dict['Input'].keys()) != 1:
                raise GeoEDFError('Connector must have exactly one Input source')
            # next check to make sure each variable binding in the pre-filters has exactly 
            # one filter
            if 'Filter' in self.__def_dict:
                for filter_param in self.__def_dict['Filter']:
                    if len(self.__def_dict['Filter'][filter_param].keys()) != 1:
                        raise GeoEDFError('Each filter parameter binding must have exactly one Filter source')
            # check that there is atmost one Output plugin
            if 'Output' in self.__def_dict:
                if len(self.__def_dict['Output'].keys()) != 1:
                    raise GeoEDFError('Connector must have exactly one Output plugin')
            # next perform parameter validations
            if self.validate_params():
                return True
            else:
                return False
        else:
            raise GeoEDFError('Connector must have an Input definition')

    # validate the stage references in the various plugins
    # need to reference earlier workflow stages
    def validate_stage_refs(self,workflow_stage):
        for plugin in self.stage_refs.keys():
            for stage_ref in self.stage_refs[plugin]:
                stage_ref_num = int(stage_ref)
                if not stage_ref_num < workflow_stage:
                    raise GeoEDFError('Invalid stage reference $%d in workflow stage $%d' % (stage_ref_num,workflow_stage))

    # determine plugin dependencies (mainly variable-filter chains)
    # encode as a dictionary of dependencies, keyed by stage identifiers
    # also collect stage references and (class) names of plugins
    # validate stage references using workflow_stage
    def identify_plugin_dependencies(self):
        # first identify the variable dependencies of each plugin
        # then convert to plugin dependencies
        # stage refs are kept as is
        # first the input plugin

        input_def = self.__def_dict['Input']

        self.plugin_names['Input'] = list(input_def.keys())[0]

        # what vars does the Input plugin depend on
        self.var_dependencies['Input'] = self.helper.collect_var_dependencies(input_def)

        # which prior stages does the Input plugin reference
        self.stage_refs['Input'] = self.helper.collect_stage_refs(input_def)

        # do we have any filters?
        if 'Filter' in self.__def_dict:
            for filtered_param in self.__def_dict['Filter']:
                filter_def = self.__def_dict['Filter'][filtered_param]

                # keep track of dependencies
                # construct Filter ID
                filter_id = 'Filter:%s' % filtered_param

                self.plugin_names[filter_id] = list(filter_def.keys())[0]

                # which filter binds this var
                self.var_filter[filtered_param] = filter_id

                # what vars does this filter depend on
                self.var_dependencies[filter_id] = self.helper.collect_var_dependencies(filter_def)

                # stages referenced by this filter
                self.stage_refs[filter_id] = self.helper.collect_stage_refs(filter_def)

        # the only dependencies can be filter plugins
        # loop through input and filter plugins

        # first the input
        self.plugin_dependencies['Input'] = []

        # this should work since we have already validated the definition
        # there can't be any unbound variables
        for var in self.var_dependencies['Input']:
            self.plugin_dependencies['Input'].append(self.var_filter[var])

        # next each filter
        for filtered_var in self.var_filter.keys():
            filter_id = 'Filter:%s' % filtered_var
            self.plugin_dependencies[filter_id] = []
            for dep_var in self.var_dependencies[filter_id]:
                self.plugin_dependencies[filter_id].append(self.var_filter[dep_var])

    # determine args bound to local files for each plugin
    # creates a dictionary mapping arg to file
    def identify_local_file_args(self):
        input_def = self.__def_dict['Input']

        # what args are bound to local files in Input plugin
        self.local_file_args['Input'] = self.helper.collect_local_file_bindings(input_def)

        # do we have any filters?
        if 'Filter' in self.__def_dict:
            for filtered_param in self.__def_dict['Filter']:
                filter_def = self.__def_dict['Filter'][filtered_param]

                # construct Filter ID
                filter_id = 'Filter:%s' % filtered_param

                # what args in Filter are bound to local files
                self.local_file_args[filter_id] = self.helper.collect_local_file_bindings(filter_def)

    # determine args with an empty binding
    # creates a dictionary mapping plugin ID to list of such args
    def identify_sensitive_args(self):
        input_def = self.__def_dict['Input']

        # what args are left blank in the Input plugin
        self.sensitive_args['Input'] = self.helper.collect_empty_bindings(input_def)

        # do we have any filters?
        if 'Filter' in self.__def_dict:
            for filtered_param in self.__def_dict['Filter']:
                filter_def = self.__def_dict['Filter'][filtered_param]

                # construct Filter ID
                filter_id = 'Filter:%s' % filtered_param

                # what args in Filter are bound to local files
                self.sensitive_args[filter_id] = self.helper.collect_empty_bindings(filter_def)

    # determine stage references with dir modifier applied to them
    # creates a dictionary mapping plugin ID to list of such stage refs
    def identify_dir_modified_args(self):
        input_def = self.__def_dict['Input']

        # what refs have dir modifiers applied to them
        self.dir_modified_refs['Input'] = self.helper.collect_dir_modified_refs(input_def)

        # do we have any filters?
        if 'Filter' in self.__def_dict:
            for filtered_param in self.__def_dict['Filter']:
                filter_def = self.__def_dict['Filter'][filtered_param]

                # construct Filter ID
                filter_id = 'Filter:%s' % filtered_param

                # what args in Filter have dir modifiers in value
                self.dir_modified_refs[filter_id] = self.helper.collect_dir_modified_refs(filter_def)

            
