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
from yaml import FullLoader
import json
import re

from .GeoEDFError import GeoEDFError
from .WorkflowUtils import WorkflowUtils
from .GeoEDFConnector import GeoEDFConnector
from .GeoEDFProcessor import GeoEDFProcessor

from Pegasus.DAX3 import *
from Pegasus.jupyter.instance import *

class WorkflowBuilder:

    CONNECTOR = 1
    PROCESSOR = 2

    # initialize builder; do any necessary init tasks
    # pass along anything that may be needed; for now, just the target execution environment
    # creates local workflow directory to hold subworkflow DAX XMLs
    # and merge result outputs

    def __init__(self,workflow_filename,target='local'):
        with open(workflow_filename,'r') as workflow_file:
            self.workflow_dict = yaml.load(workflow_file,Loader=FullLoader)
        self.workflow_filename = workflow_filename
        self.target = target

        # get a WorkflowUtils helper
        self.helper = WorkflowUtils()

        # initialize workflow DAX
        self.init_workflow()

    def count_stages(self):
        return len(self.workflow_dict.keys())

    # initialize Pegasus DAX with an admin task
    # e.g. job for creating the remote data directory for the workflow
    # also creates the local run directory for this workflow
    def init_workflow(self):

        # get a unique identifier based on epoch time
        self.workflow_id = self.helper.gen_workflow_id()
        
        # initialize DAX
        self.dax = ADAG("geoedf-%s" % self.workflow_id)

        # determine full path of new directory to hold intermediate results on target site
        self.job_dir = self.helper.target_job_dir(self.target)

        # create a local run directory; this will be used to store subdax XMLs and intermediate
        # outputs
        self.run_dir = self.helper.create_run_dir()

        # initialize the transformation catalog
        self.tc = TransformationCatalog(workflow_dir=self.run_dir)

        # add the connector container to the DAX-level TC
        conn_container = Container("geoedf-connector",type="singularity",image="library://rkalyanapurdue/connectors/geoedf:latest",mount=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        #conn_container = Container("geoedf-connector",type="singularity",image="library://rkalyana/default/geoedf:latest",mount=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        self.tc.add_container(conn_container)
        
        # create and add executables for running connector plugins

        conn_exec = Executable("run-connector-plugin",installed=True,container=conn_container)
        conn_exec.addPFN(PFN("/usr/local/bin/run-workflow-stage.sh",self.target))
        self.tc.add(conn_exec)

        merge_filter_out_exec = Executable("merge.py",installed=True,container=conn_container)
        merge_filter_out_exec.addPFN(PFN("/usr/local/bin/merge.py",self.target))
        self.tc.add(merge_filter_out_exec)

        collect_input_out_exec = Executable("collect.py",installed=True,container=conn_container)
        collect_input_out_exec.addPFN(PFN("/usr/local/bin/collect.py",self.target))
        self.tc.add(collect_input_out_exec)

        # create an executable for generating a public-private key pair
        # this is provided by the framework and will be run in the connector container
        gen_keypair_exec = Executable(name="gen_keypair", installed=True, container=conn_container)
        gen_keypair_exec.addPFN(PFN("/usr/local/bin/gen-keypair.py",self.target))
        self.tc.add(gen_keypair_exec)

        # parse processor registry and add processor containers and executables to the DAX-level TC
        hdfeosshapefilemask_cont = Container("geoedf-hdfeosshapefilemask",type="singularity",image="library://rkalyanapurdue/processors/hdfeosshapefilemask:latest",mount=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        #hdfeosshapefilemask_cont = Container("geoedf-hdfeosshapefilemask",type="singularity",image="library://rkalyana/default/geoedf-hdfeosshapefilemask",mount=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        self.tc.add_container(hdfeosshapefilemask_cont)
        hdfeosshapefilemask_exec = Executable("run-processor-hdfeosshapefilemask",installed=True,container=hdfeosshapefilemask_cont)
        hdfeosshapefilemask_exec.addPFN(PFN("/usr/local/bin/run-workflow-stage.sh",self.target))
        self.tc.add(hdfeosshapefilemask_exec)

        # executables that create connector and processor plugin subdax
        # path to executable is determined by running "which"
        # executables are "bin" scripts installed via engine package
        build_conn_plugin_subdax = Executable(name="build_conn_plugin_subdax", arch="x86_64", installed=False)
        conn_plugin_builder_path = self.helper.find_exec_path("build-conn-plugin-subdax")
        build_conn_plugin_subdax.addPFN(PFN("file://%s" % conn_plugin_builder_path,"local"))

        self.tc.add(build_conn_plugin_subdax)

        build_proc_plugin_subdax = Executable(name="build_proc_plugin_subdax", arch="x86_64", installed=False)
        proc_plugin_builder_path = self.helper.find_exec_path("build-proc-plugin-subdax")
        build_proc_plugin_subdax.addPFN(PFN("file://%s" % proc_plugin_builder_path,"local"))

        self.tc.add(build_proc_plugin_subdax)

        # executable to build final subdax that simply returns outputs
        build_final_subdax = Executable(name="build_final_subdax", arch="x86_64", installed=False)
        final_builder_path = self.helper.find_exec_path("build-final-subdax")
        build_final_subdax.addPFN(PFN("file://%s" % final_builder_path,"local"))

        self.tc.add(build_final_subdax)

        # create an executable for making directories for each workflow stage
        # needs to be at object-level since we need to use the target for the PFN
        mkdir = Executable(name="mkdir", arch="x86_64", installed=True)
        mkdir.addPFN(PFN("/bin/mkdir",self.target))
        self.tc.add(mkdir)

        # executable for final job that moves files to running dir so they can be returned
        move = Executable(name="move", arch="x86_64", installed=True)
        move.addPFN(PFN("/bin/mv",self.target))
        self.tc.add(move)

        # dummy executable for final job
        dummy = Executable(name="dummy", arch="x86_64", installed=True)
        dummy.addPFN(PFN("/bin/true",self.target))
        self.tc.add(dummy)

        # build the site catalog
        self.sc = SitesCatalog(workflow_dir=self.run_dir)
        self.sc.add_site('condorpool', arch=Arch.X86_64, os=OSType.LINUX)
        self.sc.add_site_profile('condorpool', namespace=Namespace.PEGASUS, key='style', value='condor')
        self.sc.add_site_profile('condorpool', namespace=Namespace.CONDOR, key='universe', value='vanilla')
        self.sc.add_site_profile('condorpool', namespace=Namespace.CONDOR, key='should_transfer_files', value=True)
        self.sc.add_site_profile('condorpool', namespace=Namespace.CONDOR, key='requirements', value=True)

        # build replica catalog
        self.rc = ReplicaCatalog(workflow_dir=self.run_dir)
        self.rc.add('gpg.txt', 'file:///tmp/gpg.txt', site='local')

        # leaf job used to manage dependencies across stages
        self.leaf_job = None

    # get a workflow instance using the built DAX
    def get_workflow_instance(self):
        try:
            workflow_inst = Instance(self.dax, replica_catalog=self.rc, transformation_catalog=self.tc, sites_catalog=self.sc, workflow_dir=self.run_dir)
            return workflow_inst
        except:
            raise GeoEDFError('Error creating workflow instance!!')

    # build a Pegasus DAX using the workflow-dict
    def build_pegasus_dax(self):

        # determine number of stages & loop through them creating sub-workflow for each plugin in each stage

        num_stages = len(self.workflow_dict)

        # create a initial job for making a jobdir
        make_workflow_data_dir = Job("mkdir")
        make_workflow_data_dir.addArguments("-p",self.job_dir)
        self.dax.addJob(make_workflow_data_dir)

        # create a job for generating a new public-private key pair on the target site
        # provide workflow_id as keypair filename prefix
        gen_keypair_job = Job("gen_keypair")
        gen_keypair_job.addArguments(self.job_dir)

        # set the output file so the public key is returned here for future encryption
        public_key_filename = 'public.pem'
        self.pubkey_file = File(public_key_filename)
        self.dax.addFile(self.pubkey_file)
        gen_keypair_job.uses(self.pubkey_file, link=Link.OUTPUT)
        
        self.dax.addJob(gen_keypair_job)

        # add a dependency
        self.dax.depends(parent=make_workflow_data_dir,child=gen_keypair_job)

        self.leaf_job = gen_keypair_job

        for curr_stage in range(1,num_stages+1):

            stage_data_dir = "%s/%d" % (self.job_dir,curr_stage)
            
            make_stage_data_dir = Job("mkdir")
            make_stage_data_dir.addArguments("-p",stage_data_dir)
            self.dax.addJob(make_stage_data_dir)

            # add a dependency to prior stage
            if self.leaf_job is not None:
                self.dax.depends(parent=self.leaf_job,child=make_stage_data_dir)

            stage_id = '$%d' % curr_stage

            # retrieve this workflow stage and build subdax for each plugin in it
            workflow_stage  = self.workflow_dict[stage_id]

            # some simple validation
            if len(workflow_stage.keys()) == 0:
                raise GeoEDFError("Stage %d does not have any plugins" % curr_stage)

            # naive implementation of connector | processor check
            if 'Input' in workflow_stage.keys():
                stage_type = WorkflowBuilder.CONNECTOR
            else:
                stage_type = WorkflowBuilder.PROCESSOR

            # slightly more complex in case of connector
            if stage_type == WorkflowBuilder.CONNECTOR:
                self.construct_conn_subdax(curr_stage,workflow_stage,make_stage_data_dir)
            else:
                self.construct_proc_subdax(curr_stage,workflow_stage,make_stage_data_dir)

        # finally get the outputs back from the last stage
        self.construct_final_subdax(num_stages)

    # constructs the subdax and its executor job for a connector in the workflow
    def construct_conn_subdax(self,stage_num,workflow_stage,stage_mkdir_job):
        # instantiate GeoEDFConnector object which performs validation
        # and identifies dependency chain
        conn_inst = GeoEDFConnector(workflow_stage,stage_num)

        # then loop through plugins, creating subdax for any that are fully
        # bound, until all plugins have been run

        #PYTHON2=>3
        #plugins = conn_inst.plugin_dependencies.keys()
        plugins = list(conn_inst.plugin_dependencies.keys())

        # dictionary of jobs keyed by plugin id to set up dependencies
        plugin_jobs = dict()

        # while plugins still exist
        while len(plugins) > 0:

            plugins_done = []
                    
            # find ones that are fully bound and create jobs
            # manage dependencies for these jobs, essentially vars + prior stage
            for plugin_id in plugins:
                fully_bound = True
                for dep_plugin in conn_inst.plugin_dependencies[plugin_id]:
                    # dependency not yet bound, skip
                    if dep_plugin in plugins:
                        fully_bound = False
                        break
                
                # can't process this plugin yet, try another
                if not fully_bound:
                    continue
            
                # plugin can be executed, create a subdax for this plugin
                plugins_done.append(plugin_id)

                # if this plugin has any sensitive args, prompt the user for values
                # gets a JSON back
                plugin_sensitive_args = conn_inst.sensitive_args[plugin_id]
                if len(plugin_sensitive_args) > 0:
                    sensitive_arg_binds = self.helper.collect_sensitive_arg_binds(stage_num,conn_inst.plugin_names[plugin_id],plugin_sensitive_args)
                    sensitive_arg_binds_str = json.dumps(json.dumps(sensitive_arg_binds))
                else:
                    sensitive_arg_binds_str = 'None'

                # file to hold the subdax for this plugin
                # if filter
                if ':' in plugin_id:
                    stage_name = 'stage-%d-Filter-%s' % (stage_num,plugin_id.split(':')[1])
                else: #input
                    stage_name = 'stage-%d-Input' % stage_num
                                
                subdax_filename = '%s.xml' % stage_name

                # add subdax file to DAX
                subdax_file = File(subdax_filename)
                subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
                subdax_file.addPFN(PFN("file://%s" % subdax_filepath, "local"))
                self.dax.addFile(subdax_file)

                # construct subdax for this plugin, then create a job to execute this subdax
                try:
                    # job constructing subdax for this stage
                    # stage reference and var values files are needed as input
                    dep_vars_str = self.helper.list_to_str(conn_inst.var_dependencies[plugin_id])
                    stage_refs_str = self.helper.list_to_str(conn_inst.stage_refs[plugin_id])
                    # if local args dictionary is empty
                    if not conn_inst.local_file_args[plugin_id]:
                        local_file_args_str = 'None'
                    else:
                        local_file_args_str = json.dumps(json.dumps(conn_inst.local_file_args[plugin_id]))
                    stage_id = '%d' % stage_num
                    plugin_name = conn_inst.plugin_names[plugin_id]
                    subdax_job = self.construct_plugin_subdax(stage_id, subdax_filepath, plugin_id, plugin_name, dep_vars_str, stage_refs_str,local_file_args_str, sensitive_arg_binds_str=sensitive_arg_binds_str)
                    self.dax.addJob(subdax_job)

                    # add dependencies; mkdir job and any var dependencies
                    self.dax.depends(parent=stage_mkdir_job,child=subdax_job)
                                
                    # dependencies on filters
                    for dep_plugin_id in conn_inst.plugin_dependencies[plugin_id]:
                        self.dax.depends(parent=plugin_jobs[dep_plugin_id],child=subdax_job)

                    # add job executing sub-workflow to DAX
                    subdax_exec_job = DAX(subdax_filename)
                    subdax_exec_job.addArguments("-Dpegasus.catalog.site.file=/usr/local/data/sites.xml",
                                                 "-Dpegasus.integrity.checking=none",
                                                 "--sites",self.target,
                                                 "--output-site","local",
                                                 "--basename",stage_name)
                    subdax_exec_job.uses(subdax_file, link=Link.INPUT)
                    self.dax.addDAX(subdax_exec_job)
                    # add dependency between job building subdax and job executing it
                    self.dax.depends(parent=subdax_job, child=subdax_exec_job)

                    # if this is an Input plugin, make it the leaf of this stage
                    if plugin_id == 'Input':
                        self.leaf_job = subdax_exec_job

                    # update job dictionary
                    plugin_jobs[plugin_id] = subdax_exec_job
                                
                except Exception as e:
                    raise GeoEDFError("Error constructing sub-workflow for plugin %s: %s" % (plugin_id,e))
                                                         
            # update plugins list
            for plugin_id in plugins_done:
                # remove from plugins
                plugins.remove(plugin_id)

    # constructs the subdax and its executor job for a processor in the workflow
    def construct_proc_subdax(self,stage_num,workflow_stage,stage_mkdir_job):
        # instantiate GeoEDFProcessor object which performs validation
        proc_inst = GeoEDFProcessor(workflow_stage)

        # file to hold the subdax for this plugin
        stage_name = 'stage-%d-Processor' % stage_num
                                
        subdax_filename = '%s.xml' % stage_name

        # add subdax file to DAX
        subdax_file = File(subdax_filename)
        subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
        subdax_file.addPFN(PFN("file://%s" % subdax_filepath, "local"))
        self.dax.addFile(subdax_file)

        # construct subdax for this processor plugin, then create a job to execute this subdax
        try:
            # job constructing subdax for this stage
            # stage references are needed as input
            stage_refs_str = self.helper.list_to_str(proc_inst.stage_refs)
            # if local args dictionary is empty
            if not proc_inst.local_file_args:
                local_file_args_str = 'None'
            else:
                local_file_args_str = json.dumps(json.dumps(proc_inst.local_file_args))
            stage_id = '%d' % stage_num
            plugin_name = proc_inst.plugin_name

            # if this plugin has any sensitive args, prompt the user for values
            # gets a JSON back
            plugin_sensitive_args = proc_inst.sensitive_args
            if len(plugin_sensitive_args) > 0:
                sensitive_arg_binds = self.helper.collect_sensitive_arg_binds(stage_num,plugin_name,plugin_sensitive_args)
                sensitive_arg_binds_str = json.dumps(json.dumps(sensitive_arg_binds))
            else:
                sensitive_arg_binds_str = 'None'

            subdax_job = self.construct_plugin_subdax(stage_id, subdax_filepath, plugin_name=plugin_name, stage_refs_str=stage_refs_str, local_file_args_str=local_file_args_str, sensitive_arg_binds_str=sensitive_arg_binds_str)
            self.dax.addJob(subdax_job)

            # add dependency on mkdir job
            self.dax.depends(parent=stage_mkdir_job,child=subdax_job)
                                
            # add job executing sub-workflow to DAX
            subdax_exec_job = DAX(subdax_filename)
            subdax_exec_job.addArguments("-Dpegasus.catalog.site.file=/usr/local/data/sites.xml",
                                         "-Dpegasus.integrity.checking=none",
                                         "--sites",self.target,
                                         "--output-site","local",
                                         "--basename",stage_name)
            subdax_exec_job.uses(subdax_file, link=Link.INPUT)
            self.dax.addDAX(subdax_exec_job)
            # add dependency between job building subdax and job executing it
            self.dax.depends(parent=subdax_job, child=subdax_exec_job)

            # update the leaf of the DAX to point to this job
            self.leaf_job = subdax_exec_job

        except Exception as e:
            raise GeoEDFError("Error constructing sub-workflow for stage %d: %s" % (stage_num,e))

    # constructs the subdax and its executor job for a final job that returns outputs
    def construct_final_subdax(self,num_stages):

        num_stages_str = '%d' % num_stages
        
        subdax_filename = 'final.xml'

        # add subdax file to DAX
        subdax_file = File(subdax_filename)
        subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
        subdax_file.addPFN(PFN("file://%s" % subdax_filepath, "local"))
        self.dax.addFile(subdax_file)

        # construct subdax for this final job, then create a job to execute this subdax
        try:
            # job constructing final subdax
            subdax_build_job = Job(name="build_final_subdax")
            subdax_build_job.addProfile(Profile("hints", "execution.site", "local"))

            # job arguments:
            # num stages
            # subdax filepath
            # remote job directory
            # run directory
            # target site
            subdax_build_job.addArguments(num_stages_str,subdax_filepath,self.job_dir,self.run_dir,self.target)
            self.dax.addJob(subdax_build_job)
            
            # add dependency on current leaf job
            self.dax.depends(parent=self.leaf_job,child=subdax_build_job)
                                
            # add job executing sub-workflow to DAX
            subdax_exec_job = DAX(subdax_filename)
            subdax_exec_job.addArguments("-Dpegasus.catalog.site.file=/usr/local/data/sites.xml",
                                         "-Dpegasus.integrity.checking=none",
                                         "--sites",self.target,
                                         "--output-site","local",
                                         "--basename","final")
            subdax_exec_job.uses(subdax_file, link=Link.INPUT)
            self.dax.addDAX(subdax_exec_job)
            # add dependency between job building subdax and job executing it
            self.dax.depends(parent=subdax_build_job, child=subdax_exec_job)

        except Exception as e:
            raise GeoEDFError("Error constructing sub-workflow for final stage: %s" % e)
                                                         
        
    # return job constructing subdax for a given workflow stage plugin
    # needs the workflow file, stage number, plugin ID (in case of connectors)
    # file to store subdax XML in
    # remote job directory prefix
    # target site
    # local run directory (to obtain prior stage output files)
    # var dependencies encoded as comma separated string
    # stage references encoded as comma separated string
    # args bound to local files encoded as a JSON string of arg-filepath mappings
    def construct_plugin_subdax(self,workflow_stage,subdax_filepath,plugin_id=None, plugin_name=None, var_deps_str=None,stage_refs_str=None,local_file_args_str=None,sensitive_arg_binds_str=None):

        # construct subdax and save in subdax file
        # executable to be used differs based on whether we are
        # converting a connector or processor instance
        if plugin_id is not None: # connector
            subdax_build_job = Job(name="build_conn_plugin_subdax")
            # always run this job locally
            subdax_build_job.addProfile(Profile("hints", "execution.site", "local"))

            # job arguments:
            # workflow filepath
            # stage identifier
            # plugin identifier
            # subdax filepath
            # remote job directory
            # run directory
            # dependant vars as comma separated string
            # stage references as comma separated string
            # arg bindings to local files as JSON string
            # target site
            subdax_build_job.addArguments(self.workflow_filename,workflow_stage,plugin_id,plugin_name,subdax_filepath,self.job_dir,self.run_dir,var_deps_str,stage_refs_str,local_file_args_str,sensitive_arg_binds_str,self.target)
            
            return subdax_build_job

        else:
            subdax_build_job = Job(name="build_proc_plugin_subdax")

            # always run this job locally
            subdax_build_job.addProfile(Profile("hints", "execution.site", "local"))

            # job arguments:
            # workflow filepath
            # stage identifier
            # plugin identifier
            # subdax filepath
            # remote job directory
            # run directory
            # dependant vars as comma separated string
            # stage references as comma separated string
            # arg bindings to local files as JSON string
            # target site
            subdax_build_job.addArguments(self.workflow_filename,workflow_stage,plugin_name,subdax_filepath,self.job_dir,self.run_dir,stage_refs_str,local_file_args_str,sensitive_arg_binds_str,self.target)
            
            return subdax_build_job

            
            

