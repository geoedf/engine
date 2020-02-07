#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Top-level connector class; implements the methods for parsing a connector 
    instance YAML, validating it, and deriving the dependency graph
"""

import sys
import os
import yaml
import re
import importlib
import itertools
import getpass
import random
from functools import reduce

from ..helper.GeoEDFError import GeoEDFError

class GeoEDFConnector:
    
    # can either provide a standalone YAML file defining the connector, or
    # provide a dictionary that is a connector piece of a workflow
    # file takes precedence; ignore dict if both provided
    def __init__(self,def_filename=None,def_dict=None):
        
        # if file is provided, takes precedence
        if def_filename is not None:
            with open(def_filename,'r') as yamlfile:
                # parse the YAML file & load the definition 
                cfg = yaml.load(yamlfile)
                self.__def_dict = cfg
        # if dictionary provided
        elif def_dict is not None:
            self.__def_dict = def_dict
        else: # neither definition file, nor dictionary provided
            raise GeoEDFError('Connector objects need a definition')

        # validate the definition
        # will raise an exception if this fails
        if self.validate_definition():
            # first setup the output path
            random.seed()
            self.target_path = '/tmp/%d' % (random.randrange(1000))
            while os.path.exists(self.target_path):
                self.target_path = '/tmp/%d' % (random.randrange(1000))

            os.makedirs(self.target_path)
                
            # initialize a few dictionaries to hold the dependencies for a plugin & pre-filter bindings
            self.var_filtered = dict()
            self.var_dependencies = dict()
            # now construct the various connector plugin objects
            self.build_connector_plugins()

    # parses a string to find the mentioned variables: %{var}
    def find_dependent_vars(self,value):
        if value is not None and isinstance(value, str):
            return re.findall('\%\{(.+?)\}',value)
        else:
            return []

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
            param_vars = self.find_dependent_vars(param_val)
            if len(param_vars) > 0 and section in ('Post-Filter','Output'):
                raise GeoEDFError('Variables not allowed in post filters or outputs')
            else:
                for var in self.find_dependent_vars(param_val):
                    if var in unbound_vars:
                        raise GeoEDFError('Cannot reuse variable: %s' % var)
                    unbound_vars.append(var)
        # check that variables are not also bound parameters
        # have to use names distinct from 'reserved' plugin parameter names
        # first update the set of bound params
        bound_params += plugin_params
        if set(bound_params).intersection(set(unbound_vars)):
            raise GeoEDFError('A variable cannot also be a bound plugin parameter')

        return [bound_params,unbound_vars]
            
        
    # validates the connector definition by making sure that parameters are bound just once,
    # variables are not reused, and that each variable is bound
    # also implies that post filters & output plugin cannot contain any variables
    def validate_params(self):
        # keep a list of vars unbound so far; any new binding has to be of one of these vars
        # since var names cannot be reused across plugins to avoid confusion, one list is sufficient
        # also keep a list of bound vars to check that all variables have been bound at the end
        unbound_vars = []
        bound_vars = []
        bound_params = []
        
        try:

            # each plugin type has its own structure, needs special processing
            for section in self.__def_dict:
                if section == 'Input':
                    for input_plugin in self.__def_dict[section]:
                        [bound_params,unbound_vars] = self.validate_plugin_def(self.__def_dict[section][input_plugin], \
                                                                               bound_params,unbound_vars,'Input')
            
                if section == 'Filter':
                    if 'Pre' in self.__def_dict[section]:
                        # first process the pre-filters, some of these params can be input params
                        for filtered_param in self.__def_dict[section]['Pre']:
                            # if an input param bound by a pre filter was already bound in the input definition, then raise error
                            if filtered_param not in unbound_vars:
                                raise GeoEDFError('Only variables can be bound by a pre-filter: %s' % filtered_param)
                            elif filtered_param in bound_vars:
                                raise GeoEDFError('A variable can only be bound once by a pre-filter: %s' % filtered_param)
                            else: # add this to the set of bound variables
                                bound_vars.append(filtered_param)
                            
                            # get this parameter's filter definition
                            for param_pre_filter in self.__def_dict[section]['Pre'][filtered_param]:
                                [bound_params,unbound_vars] = self.validate_plugin_def( \
                                                                self.__def_dict[section]['Pre'][filtered_param][param_pre_filter], \
                                                                bound_params,unbound_vars,'Pre-Filter')

                    # now process the post filters
                    if 'Post' in self.__def_dict[section]:
                        for post_filter in self.__def_dict[section]['Post']:
                            [bound_params,unbound_vars] = self.validate_plugin_def( \
                                                            self.__def_dict[section]['Post'][post_filter], \
                                                            bound_params,unbound_vars,'Post-Filter')

                if section == 'Output':
                    # now process the output plugin
                    for output_plugin in self.__def_dict[section]:
                        [bound_params,unbound_vars] = self.validate_plugin_def( \
                                                        self.__def_dict[section][output_plugin], \
                                                        bound_params,unbound_vars,'Output')

            # make sure all variables have been bound
            if len(bound_vars) != len(unbound_vars):
                raise GeoEDFError('All variables need to be bound by pre filters')

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
                for pre_filter_param in self.__def_dict['Filter']['Pre']:
                    if len(self.__def_dict['Filter']['Pre'][pre_filter_param].keys()) != 1:
                        raise GeoEDFError('Each pre filter parameter binding must have exactly one Filter source')
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

    # build a particular connector plugin given its definition dictionary & type
    # the type is used to construct the module path
    # also keep track of the unbound variables in this plugin to instantiate them
    # with all possible filter values
    def build_connector_plugin(self,plugin_def_dict,plugin_type):
        plugin_cname = 'unknown'
        try:
            # get the class name for the plugin & import it
            (plugin_cname,plugin_inst), = plugin_def_dict.items()
        
            #get a dictionary of parameters & their values
            plugin_params = plugin_def_dict[plugin_cname]

            # construct module name
            plugin_mname = 'GeoEDF.connector.%s.%s' % (plugin_type,plugin_cname)

            plugin_module = importlib.import_module(plugin_mname)
            PluginClass = getattr(plugin_module,plugin_cname)

            plugin_obj = PluginClass(**plugin_params)

            return plugin_obj
        except:
            raise GeoEDFError('Error constructing %s plugin %s' % (plugin_type,plugin_cname))
        

    # build the individual connector plugin objects based on the definition and add them to a 
    # dictionary on the top level connector object for ease of access & reference
    # pre filter plugins are not directly accessible, but stored as keys of a var_filtered dictionary
    # that has a mapping between the pre-filter and the variable it binds
    def build_connector_plugins(self):
        # first the input plugin
        input_def = self.__def_dict['Input']
        
        self.input_plugin = self.build_connector_plugin(input_def,'input')

        # set the target path to store files
        self.input_plugin.target_path = self.target_path

        self.var_dependencies[self.input_plugin] = []

        # keep track of variable dependencies so that parameters can be instantiated when the 
        # variable is bound in the corresponding filter
        for unbound_var in self.input_plugin.rev_dependencies:
            self.var_dependencies[self.input_plugin].append(unbound_var)

        # do we have any filters?
        if 'Filter' in self.__def_dict:
            if 'Pre' in self.__def_dict['Filter']:
                for filtered_param in self.__def_dict['Filter']['Pre']:
                    pre_filter_def = self.__def_dict['Filter']['Pre'][filtered_param]
                    filter_plugin = self.build_connector_plugin(pre_filter_def,'filter')

                    # keep track of dependencies
                    self.var_dependencies[filter_plugin] = []
                    for unbound_var in filter_plugin.rev_dependencies:
                        self.var_dependencies[filter_plugin].append(unbound_var)
                    self.var_filtered[filter_plugin] = filtered_param

    # execute this connector
    # loop through various values provided by pre-filters, invoking the input plugin each time 
    def execute(self):
        # dictionary to hold variable bindings
        var_bindings = dict()

        # loop until all filters have been fully instantiated
        while(self.var_filtered):
            for filter_plugin in self.var_filtered:
                # if each variable dependency of the plugin has been bound
                if len(self.var_dependencies[filter_plugin]) == 0 or \
                       reduce((lambda x,y: x and y),[unbound_var in var_bindings for unbound_var in self.var_dependencies[filter_plugin]]):
                    filtered_param = self.var_filtered[filter_plugin]

                    if len(self.var_dependencies[filter_plugin]) == 0:
                        var_bindings[filtered_param] = filter_plugin.filter()

                    else:

                        # get the set of all possible variable binding combinations
                        var_combos = list(itertools.product(*list(map((lambda var: var_bindings[var]),self.var_dependencies[filter_plugin]))))

                        var_bindings[filtered_param] = []

                        for var_combo in var_combos:
                            # first reset param bindings to original values
                            filter_plugin.reset_bindings()
                            for indx in range(0,len(self.var_dependencies[filter_plugin])):
                                filter_plugin.set_param(self.var_dependencies[filter_plugin][indx],var_combo[indx])
                            var_bindings[filtered_param] = list(set(var_bindings[filtered_param]).union(set(filter_plugin.filter())))

                        if len(var_bindings[filtered_param]) == 0:
                            raise GeoEDFError('Connector cannot be run since variable %s has no valid values' % filtered_param)

                    # delete the entry from var_filtered
                    del(self.var_filtered[filter_plugin])
                    break

        # now process the input plugin
        # first check to see if a password param is included, if so prompt user for value
        if 'password' in self.input_plugin.provided_params:
            self.input_plugin.set_param('password',getpass.getpass(prompt='Enter password for input plugin: '))

        # process all variable bindings, calling input plugin's get method for each binding
        if len(self.var_dependencies[self.input_plugin]) == 0 or \
           reduce((lambda x,y: x and y),[unbound_var in var_bindings for unbound_var in self.var_dependencies[self.input_plugin]]):
            if len(self.var_dependencies[self.input_plugin]) == 0:
                self.input_plugin.get()
            else:
                # get the set of all possible variable binding combinations
                var_combos = list(itertools.product(*list(map((lambda var: var_bindings[var]),self.var_dependencies[self.input_plugin]))))

                if len(var_combos) == 0:
                    raise GeoEDFError('Input plugin cannot be run since one or more variables do not have any bindings')

                for var_combo in var_combos:
                    # first reset param bindings with variables to original values
                    self.input_plugin.reset_bindings()
                    for indx in range(0,len(self.var_dependencies[self.input_plugin])):
                        self.input_plugin.set_param(self.var_dependencies[self.input_plugin][indx],var_combo[indx])
                    self.input_plugin.get()
            print('All files have been downloaded to %s' % self.target_path)
        else:
            raise GeoEDFError('Input plugin cannot be run since one or more variables do not have any bindings')
