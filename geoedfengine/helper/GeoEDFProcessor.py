#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Top-level processor class; implements the methods for parsing a processor
    definition YAML, instantiating the necessary processor class & executing 
    the processor. Assumes that there are no variables in any of the param values.
"""

import sys
import os
import yaml
import re
import regex
import importlib
import itertools
import random
from functools import reduce

from GeoEDFError import GeoEDFError

class GeoEDFProcessor:

    # set of valid modifiers that can be applied to a data binding
    valid_modifiers = ('dir')
    
    # can either provide a standalone YAML file defining the processor, or
    # provide a dictionary that is a processor piece of a workflow
    # file takes precedence; ignore dict if both provided
    # data_bindings are used only in "workflow" mode, when data from a 
    # previous step is fed into the processor; this is a dictionary keyed 
    # by numbers; processor arguments can reference these bindings using 
    # the variable name ${n}, where n is a number.
    def __init__(self,def_filename=None,def_dict=None,data_bindings=None):
        
        # if file is provided, takes precedence
        if def_filename is not None:
            with open(def_filename,'r') as yamlfile:
                # parse the YAML file & load the definition 
                cfg = yaml.load(yamlfile)
                self.__def_dict = cfg
        # if dictionary provided
        elif def_dict is not None:
            self.__def_dict = cfg
        else: # neither definition file, nor dictionary provided
            raise GeoEDFError('Processor objects need a definition')

        # set the data bindings to the object
        self.data_bindings = data_bindings

        # params filtered by a filter plugin
        self.param_filtered = dict()

        # dictionary to hold param bindings
        self.param_bindings = dict()

        # processor object; initialize to None
        self.processor_obj = None

        # validate the definition
        # will raise an exception if this fails
        if self.validate_definition():
            # first setup the output path
            random.seed()
            self.target_path = '/tmp/%d' % (random.randrange(1000))
            while os.path.exists(self.target_path):
                self.target_path = '/tmp/%d' % (random.randrange(1000))

            os.makedirs(self.target_path)

            print('processor output located at %s' % self.target_path)

            # now execute the processor
            self.execute()
        else:
            print('error validating processor definition')

    # parses a string to find the mentioned data bindings
    def find_data_bindings(self,value):
        if value is not None and isinstance(value, str):
            return re.findall('(\$[0-9]+)',value)
        else:
            return []

    # gets all the modifiers applied to a data binding
    # assume that the value argument has been validated already
    def get_all_modifiers(self,value,mods=[]):
        if '(' in value:
            next_paren = value.index('(')
            mod = value[0:next_paren]
            return self.get_all_modifiers(value[next_paren+1:],mods.append(mod))
        else:
            return mods

    # check if list of modifiers are all valid
    def all_valid_modifiers(self,mods):
        reduce(lambda x,y: x and y,
               list(map(lambda mod: mod in self.valid_modifiers,modifiers)))

    # check if any variables are present
    # do not allow data bindings in filter argument values
    def validate_param_value(self,value,allow_data_bindings=True,allow_complex_binding=True):
        if value is None:
            raise GeoEDFError('Parameter value cannot be null')
        else:
            if isinstance(value, str):
                vars = re.findall('\%\{(.+?)\}',value)
                if len(vars) > 0:
                    raise GeoEDFError('Parameter value cannot contain variables')
                else:
                    # process data bindings, if any
                    data_binds = self.find_data_bindings(value)
                    if len(data_binds) == 0:
                        return
                    else:
                        if not allow_data_bindings:
                            raise GeoEDFError('Data bindings not allowed in filter parameter bindings')
                        else:
                            # exactly one data binding is allowed and param value needs to be the data binding 
                            # itself or a nested set of modifier(s) applied to it
                            if len(data_binds) > 1:
                                raise GeoEDFError('A parameter can only refer to exactly one data binding')
                            else:
                                # param value either has to be the data binding or a modifier applied to it
                                if value == data_binds[0]:
                                    return
                                else:
                                    match_result = regex.search(r'''(?<rec>[^()]++\((?:\$[0-9]+|(?&rec))\))''',value)
                                    modified_bindings = match_result.captures('rec')
                                    if len(modified_bindings) > 0:
                                        if value not in modified_bindings:
                                            raise GeoEDFError('Ill-formed data binding in parameter value')
                                        else:
                                            # check to make sure only valid modifiers have been used
                                            modifiers = self.get_all_modifiers(value)
                                            if not all_valid_modifiers(modifiers):
                                                raise GeoEDFError('An invalid modifer has been applied to the data binding')
                                    else:
                                        raise GeoEDFError('Ill-formed data binding in parameter value')
            elif isinstance(value, dict):
                if not allow_complex_binding:
                    raise GeoEDFError('Nested filtering is not allowed in processor parameter bindings')
                (ignore,binding_dict) = next(iter(value.items()))
                for ignore,val in binding_dict.items():
                    self.validate_param_value(val,False,False)
            elif isinstance(value, list):
                return
            else:
                raise GeoEDFError('Processor parameter value %s has an unsupported type' % value)
    
    # validates a processor definition
    # param bindings cannot have variable except for numeric data binding
    # only allow direct data binding or a pre-defined set of modifiers applied to a data binding
    # params can also be bound by filters to provide multiple bindings
    def validate_definition(self):
        # assume there's just one processor in the definition
        # get the first and only key
        self.processor_name = next(iter(self.__def_dict.keys()))
        
        self.processor_def = self.__def_dict[self.processor_name]

        self.processor_params = list(self.processor_def.keys())

        # check that no param is bound more than once
        if len(list(self.processor_params)) != len(list(set(list(self.processor_params)))):
            raise GeoEDFError('Processor parameters can only be bound once')

        # validate the parameter bindings
        for processor_param in self.processor_params:
            param_val = self.processor_def[processor_param]
            if param_val is None:
                raise GeoEDFError('Parameter must have a binding if included in definition %s' % processor_param)
            # validate this parameter value
            # look for variables in binding, if so raise error
            # also checks to see if any data bindings are present in any filter params
            self.validate_param_value(param_val)

        return True

    # build a particular filter plugin given its definition dictionary
    def build_filter_plugin(self,filter_plugin_dict):
        plugin_cname = 'unknown'
        try:
            # get the class name for the plugin & import it
            (plugin_cname,plugin_inst), = filter_plugin_dict.items()
        
            #get a dictionary of parameters & their values
            plugin_params = filter_plugin_dict[plugin_cname]

            # construct module name
            plugin_mname = 'GeoEDF.connector.filter.%s' % plugin_cname

            plugin_module = importlib.import_module(plugin_mname)
            PluginClass = getattr(plugin_module,plugin_cname)

            plugin_obj = PluginClass(**plugin_params)

            return plugin_obj
        except:
            raise GeoEDFError('Error constructing filter plugin %s' % plugin_cname)

    # return a dictionary of param name and binding values
    def build_param_bindings(self):
        # loop through the parameters
        for processor_param in self.processor_params:
            param_val = self.processor_def[processor_param]

            # if the value is a string
            if isinstance(param_val,str):
                # if value contains a data binding, process it
                data_binds = self.find_data_bindings(param_val)

                if len(data_binds) > 0:
                    data_bind = data_binds[0]
                    # initialize to value from the provided data binding dictionary
                    param_binding = self.data_bindings[data_bind]
                    # apply the nested (if any) modifiers inside out
                    mods = self.get_all_modifiers(param_val)
                    for indx in range(0,len(mods)):
                        next_inner_mod = mods[len(mods)-indx-1]
                        param_binding = apply_mod(param_binding,next_inner_mod)
                    self.param_bindings[processor_param] = [param_binding]
                else: # no data bindings
                    self.param_bindings[processor_param] = [param_val]
            # value is a dictionary, i.e. a filter
            elif isinstance(param_val,dict):
                filter_plugin = self.build_filter_plugin(param_val)
                self.param_bindings[processor_param] = filter_plugin.filter()
            elif isinstance(param_val, list):
                self.param_bindings[processor_param] = [param_val]

    # construct all possible combinations of the parameter bindings
    def build_param_combos(self):
        self.param_combos = list(itertools.product(*list(map((lambda param: self.param_bindings[param]),self.processor_params))))
        print('number of param combos %s' % len(self.param_combos))
            
    # execute this processor
    # some parameters may be bound by filters; need to loop through the various values 
    # provided by the binding filter and re-run the processor for each param binding choice
    def execute(self):

        # first get the individual parameter bindings
        self.build_param_bindings()

        # next build all possible combinations of parameter values
        self.build_param_combos()

        # now loop through the param combos and execute the processor
        for param_combo in self.param_combos:
            print(param_combo)
            # create a dictionary of param name, value
            processor_params = dict()
            for indx in range(0,len(self.processor_params)):
                processor_params[self.processor_params[indx]] = param_combo[indx]

            # if processor object doesn't exist yet, construct it; else set new param vals
            if self.processor_obj is None:
                # construct module name
                processor_mname = 'GeoEDF.processor.%s' % self.processor_name
                processor_module = importlib.import_module(processor_mname)
                ProcessorClass = getattr(processor_module,self.processor_name)

                self.processor_obj = ProcessorClass(**processor_params)
            else:
                for param in self.processor_params:
                    setattr(processor_obj,param,processor_params[param])

            #processor_obj.process()

