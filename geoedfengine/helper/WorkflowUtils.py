#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Provides low-level utilities used by the WorkflowBuilder class
    Also provides a wrapper for interacting with the middleware (broker)
    responsible for executing and monitoring workflows
"""

import sys
import os
import re
import itertools
import subprocess
import time
from getpass import getpass
from ..GeoEDFConfig import GeoEDFConfig
from .SubmitBroker import SubmitBroker

geoedf_cfg = GeoEDFConfig()
if geoedf_cfg.config is not None:
    os.environ['SREGISTRY_CLIENT'] = geoedf_cfg.config['REGISTRY']['registry_client']
    os.environ['SREGISTRY_REGISTRY_BASE'] = geoedf_cfg.config['REGISTRY']['registry_base']

from sregistry.main import get_client

from .GeoEDFError import GeoEDFError

class WorkflowUtils:

    def __init__(self):
        self.cfg = GeoEDFConfig()

    # generate a unique ID based on the epoch time
    # the ID is used as a suffix for all workflow and job directories
    def gen_workflow_id(self):
        self.workflow_id = str(int(time.time()))
        return self.workflow_id

    # finds the path to an executable by running "which"
    def find_exec_path(self,exec_name,target='condorpool'):
        try:
            # for local and condor execution, find local path
            if target == 'condorpool' or target == 'local':
                ret = dict()
                exec_path = subprocess.check_output(["which",exec_name])
                ret['exec_path'] = exec_path.decode('utf-8').rstrip()
                ret['python_path'] = os.getenv('PYTHONPATH')
                return ret
            else: # need to determine path on submit host since that is the "local" site
                # if submit configuration is provided, determine path from there
                if 'submit' in self.cfg.config:
                    ret = dict()
                    exec_path = '%s/%s' % (self.cfg.config['submit']['exec_path'],exec_name)
                    ret['exec_path'] = exec_path
                    ret['python_path'] = self.cfg.config['submit']['python_path']
                    return ret
                else:
                    raise GeoEDFError("Submit execution path not provided in config; this is required for non-local executions")
        except subprocess.CalledProcessError:
            raise GeoEDFError("Error occurred in finding the executable %s. This should not happen if the workflow engine was successfully installed!!!" % exec_name)

    # determine fully qualified path to job directory for given execution target
    def target_job_dir(self,target):
        if target == 'local' or target == 'condorpool':
            return '/data/%s' % self.workflow_id
        # else, find workflow scratch path in config
        else:
            if self.cfg.config is not None:
                site_scratch_path = self.cfg.config[target]['scratch_path']
                return '%s/%s' % (site_scratch_path,self.workflow_id)
            else:
                raise GeoEDFError("Config file not present, cannot determine appropriate job directory on execution host")

    # determine various config parameters necessary for creating TC for given execution target
    # submit host parameters are returned only for non-local targets
    def target_tc_config(self,target):
        if target == 'local' or target == 'condorpool':
            return None
        else:
            res = dict()
            if self.cfg.config is not None:
                res['os_release'] = self.cfg.config[target]['os_release']
                res['os_version'] = self.cfg.config[target]['os_version']
                return res
            else:
                return None

    # create a run directory for this workflow
    def create_run_dir(self):
        # create under home directory
        if os.getenv('HOME') is not None:
            full_path = '%s/geoedf/workflows/%s' % (os.getenv('HOME'),self.workflow_id)
            try:
                mkdir_proc = subprocess.call(["mkdir","-p",full_path])
                # set environment variable
                os.environ["RUN_DIR"] = full_path
                return full_path
            except subprocess.CalledProcessError:
                raise GeoEDFError("Error occurred in creating run directory for this workflow!!!")
        else:
           raise GeoEDFError("Could not determine user home directory; cannot create workflow directory!!!")        

    # validate this workflow
    # semantic validation occurs when processing each plugin
    # this function only performs a cursory syntactic check
    def validate_workflow(self,workflow_dict):
        # ensure that workflow stages are numeric and in order
        workflow_stages = list(workflow_dict.keys())

        # find number of stages; each $n needs to be present
        num_stages = len(workflow_stages)

        for stage_num in range(1,num_stages+1):
            stage_id = '$%d' % stage_num
            if stage_id not in workflow_stages:
                raise GeoEDFError('Workflow stages need to be in numeric order and of the form $n')

    # parses a string to find the mentioned variables: %{var}
    def find_dependent_vars(self,value):
        if value is not None and isinstance(value, str):
            return re.findall('\%\{(.+)\}',value)
        else:
            return []

    # parses a string to find stage references: $#
    def find_stage_refs(self,value):
        if value is not None and isinstance(value,str):
            return re.findall('\$([0-9]+)',value)
        else:
            return []

    # checks to make sure value is of format dir(dir(....$n)...)
    # could have used a parser here, instead do a lazy recursive check
    # will work first time since we only call if starts with dir(
    def validate_dir_modifiers(self,value,kernel):
        subexp = re.findall('dir\((.+)\)',value)
        # needs to be exactly one or equal to kernel
        if len(subexp) > 0:
            if len(subexp) > 1:
                return False
            else: # exactly one
                # first ensure this is the right subexpression
                reconstruction = 'dir(%s)' % subexp[0]
                if value != reconstruction:
                    return False
                if subexp[0].startswith('dir('): #recurse
                    return self.validate_dir_modifiers(subexp[0],kernel)
                else:
                    if subexp[0] != kernel:
                        return False
                    return True
        else:
            return False

    # validates stage refs (if they exist) in value
    # only allows exactly one stage ref and zero or more dir modifiers
    def validate_stage_refs(self,value):
        if value is not None and isinstance(value,str):
            # first find the stage refs
            stage_refs = self.find_stage_refs(value)
            if len(stage_refs) > 0: #stage refs exist
                if len(stage_refs) > 1:
                    raise GeoEDFError("Cannot reference more than one stage in a binding")
                # validate value to ensure it has stage ref with zero or more modifiers
                stage_ref_str = '$%s' % stage_refs[0]
                if value.startswith('dir('): # one or more dir modifiers exist
                    if not self.validate_dir_modifiers(value,stage_ref_str):
                        raise GeoEDFError("Error validating stage reference %s" % value)
                else: # dir modifier is not a prefix, need to be exactly the stage reference
                    if value != stage_ref_str:
                        raise GeoEDFError("stage reference has to have zero or more dir modifiers applied to it")
            else:
                return True

    # collect var dependencies for a plugin instance
    # finds binding values for each argument (key in dict) and extracts variables
    def collect_var_dependencies(self,plugin_def):
        var_deps = []
        for plugin_class in plugin_def.keys():
            plugin_inst = plugin_def[plugin_class]
            for arg in plugin_inst.keys():
                val = plugin_inst[arg]
                val_vars = self.find_dependent_vars(val)
                var_deps = list(set(var_deps).union(set(val_vars)))
        return var_deps

    # collects stage references in a plugin instance
    def collect_stage_refs(self,plugin_def):
        refs = []
        for plugin_class in plugin_def.keys():
            plugin_inst = plugin_def[plugin_class]
            for arg in plugin_inst.keys():
                val = plugin_inst[arg]
                val_stage_refs = self.find_stage_refs(val)
                refs = list(set(refs).union(set(val_stage_refs)))
        return refs

    # collect the stage references who have a dir modifier applied
    # in this plugin's bindings
    def collect_dir_modified_refs(self, plugin_def):
        dir_mod_refs = []
        for plugin_class in plugin_def.keys():
            plugin_inst = plugin_def[plugin_class]
            for arg in plugin_inst.keys():
                val = plugin_inst[arg]
                if val is not None and isinstance(val,str):
                    if val.startswith('dir('):
                        # find the stage references in this value
                        val_stage_refs = self.find_stage_refs(val)
                        dir_mod_refs = list(set(dir_mod_refs).union(set(val_stage_refs)))
        return dir_mod_refs

    # uses naive identification of local files - either has an extension or / separator
    # checks to see if these are actually files on this host and add to dictionary
    def is_local_file(self,val_str):
        if '.' in val_str or '/' in val_str: # hardcoded path separator
            if os.path.isfile(val_str):
                return True
        return False

    # collects args bound to local files
    def collect_local_file_bindings(self,plugin_def):
        file_binds = dict()
        for plugin_class in plugin_def.keys():
            plugin_inst = plugin_def[plugin_class]
            for arg in plugin_inst.keys():
                val = plugin_inst[arg]
                if isinstance(val,str) and self.is_local_file(val): #lazy eval
                    file_binds[arg] = val
        return file_binds

    # collects args with empty bindings
    def collect_empty_bindings(self,plugin_def):
        empty_args = []
        for plugin_class in plugin_def.keys():
            plugin_inst = plugin_def[plugin_class]
            for arg in plugin_inst.keys():
                val = plugin_inst[arg]
                if val is None:
                    empty_args.append(arg)
                else:
                    if isinstance(val,str) and (len(val) == 0):
                        empty_args.append(arg)
        return empty_args

    # converts a list into a comma separated string
    def list_to_str(self,val_list):
        if len(val_list) > 0:
            ret_str = '%s' % val_list[0]
            for val in val_list[1:]:
                ret_str = '%s,%s' % (ret_str,val)
            return ret_str
        else:
           return 'None'

    # creates binding combinations from two dictionaries of binding lists
    # will return an array of pairs of dictionaries
    # binding_combs({'a':[1,2],'b':[3,4]},{1:[a,b],2:[d,e]})
    # => [[{'a':1,'b':3},{1:a,2:d}],[{'a':1,'b':4},{1:a,2:d}],...]
    # also works with just dict1 provided
    def create_binding_combs(self,dict1,dict2):
        if dict1 is not None and dict2 is not None:
            # first get a listing of key to convert back into dict
            keys1 = list(dict1.keys())
            keys2 = list(dict2.keys())

            # get a cross product of first dictionary
            dict1_vals = []
            for key in keys1:
                dict1_vals.append(dict1[key])

            dict1_combs = list(itertools.product(*list(dict1_vals)))

            # get a cross product of the 2nd dictionary
            dict2_vals = []
            for key in keys2:
                # if key is to be overridden, modify values
                if key in dict2_unary_keys:
                    dict2_vals.append([dict2[key][0]])
                else:
                    dict2_vals.append(dict2[key])

            dict2_combs = list(itertools.product(*list(dict2_vals)))

            # now combine the two
            dict1_dict2_combs = list(itertools.product(dict1_combs,dict2_combs))

            # now convert this into a list of pairs of dictionaries
            binding_combs = []
            for comb_pair in dict1_dict2_combs:
                dict1_inst = dict()
                dict2_inst = dict()
                for indx in range(0,len(keys1)):
                    dict1_inst[keys1[indx]] = comb_pair[0][indx]
                for indx in range(0,len(keys2)):
                    dict2_inst[keys2[indx]] = comb_pair[1][indx]
                binding_combs.append((dict1_inst,dict2_inst))
            return binding_combs
        # if only one dict provided, return array of dicts
        elif dict1 is not None:
            # first get a listing of key to convert back into dict
            keys1 = list(dict1.keys())

            # get a cross product of the dictionary
            dict1_vals = []
            for key in keys1:
                dict1_vals.append(dict1[key])
                
            dict1_combs = list(itertools.product(*list(dict1_vals)))

            # now convert this into a list of dictionaries
            binding_combs = []
            for comb_pair in dict1_combs:
                dict1_inst = dict()
                for indx in range(0,len(keys1)):
                    dict1_inst[keys1[indx]] = comb_pair[indx]
                binding_combs.append(dict1_inst)
            return binding_combs

    # function that prompts the user for values for sensitive args
    # returns a JSON with arg-value bindings
    # uses stage_num, plugin_name to assist user
    def collect_sensitive_arg_binds(self,stage_num,plugin_name,args):
        arg_binds = dict()
        plugin_prompt = 'plugin %s in workflow stage %d' % (plugin_name,stage_num)
        for arg in args:
            arg_prompt = 'Enter value for %s in %s: ' % (arg,plugin_prompt)
            arg_binds[arg] = getpass(prompt=arg_prompt)
        return arg_binds

    # function that queries the Singularity registry server for containers
    # in the connector and processor collections
    # returns names and URI in separate dictionaries
    def get_registry_containers(self):
        cli = get_client(quiet=True)

        conns = dict()
        query_res = cli.search("connectors")
        for (cont_uri,url) in query_res:
            cont_path = cont_uri.split(':')[0]
            plugin_name = cont_path.split('/')[1]
            if plugin_name not in conns:
                conns[plugin_name] = cont_uri

        procs = dict()
        query_res = cli.search("processors")
        for (cont_uri,url) in query_res:
            cont_path = cont_uri.split(':')[0]
            plugin_name = cont_path.split('/')[1]
            if plugin_name not in procs:
                procs[plugin_name] = cont_uri

        return (conns,procs)

    # function to execute a workflow via the broker
    # in the case of HUBzero submit, the command line will be used
    # to run the necessary submit commands
    def execute_workflow(self,workflow_dir,broker):
        if broker == 'submit':
            submitBroker = SubmitBroker()
            submitBroker.plan_and_submit(workflow_dir)

    # function to monitor a workflow's status via the broker
    def monitor_workflow(self,workflow_dir,broker):
        if broker == 'submit':
            submitBroker = SubmitBroker()
            submitBroker.monitor_status(workflow_dir)

