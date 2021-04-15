#!/usr/bin/env python3
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

from Pegasus.api import *

class WorkflowBuilder:

    CONNECTOR = 1
    PROCESSOR = 2

    # initialize builder; do any necessary init tasks
    # pass along anything that may be needed; for now, just the target execution environment
    # creates local workflow directory to hold subworkflow DAX YAMLs
    # and merge result outputs
    # mode is between prod and dev; in dev mode, local containers are allowed in dev mode

    def __init__(self,workflow_filename,mode='prod',target='condorpool'):
        with open(workflow_filename,'r') as workflow_file:
            self.workflow_dict = yaml.load(workflow_file,Loader=FullLoader)
        self.workflow_filename = workflow_filename
        self.target = target

        self.mode = mode

        # get a WorkflowUtils helper
        self.helper = WorkflowUtils()

        # initialize workflow DAX
        self.init_workflow()

    def count_stages(self):
        return len(self.workflow_dict.keys())

    # construct the transformation catalog (TC)
    def build_transformation_catalog(self):
        # initialize the transformation catalog
        self.tc = TransformationCatalog()

        # specific sites may need additional parameters like OS version, release etc.
        # so that the executable can match the site entry
        # fetch these for target from config file
        tc_cfg = self.helper.target_tc_config(self.target)

        # Create executables and add them to the TC
        # first a few common executables

        # these are provided by the workflowutils container
        utils_container = Container("workflowutils",
                                    Container.SINGULARITY,
                                    image="library://framework/workflowutils:latest",
                                    mounts=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        self.tc.add_containers(utils_container)

        # create an executable for merging outputs of filters
        if tc_cfg is not None:
            merge_filter_out_exec = Transformation("merge.py",
                                                   site=self.target,
                                                   pfn="/usr/local/bin/merge.py",
                                                   is_stageable=False,
                                                   arch=Arch.X86_64,
                                                   os_type=OS.LINUX,
                                                   os_release=tc_cfg['os_release'],
                                                   os_version=tc_cfg['os_version'],
                                                   container=utils_container)
        else:
            merge_filter_out_exec = Transformation("merge.py",
                                                   site=self.target,
                                                   pfn="/usr/local/bin/merge.py",
                                                   is_stageable=False,
                                                   container=utils_container)

        # create an executable for collecting the list of names of files that have been produced by a plugin
        if tc_cfg is not None:
            collect_input_out_exec = Transformation("collect.py",
                                                    site=self.target,
                                                    pfn="/usr/local/bin/collect.py",
                                                    is_stageable=False,
                                                    arch=Arch.X86_64,
                                                    os_type=OS.LINUX,
                                                    os_release=tc_cfg['os_release'],
                                                    os_version=tc_cfg['os_version'],
                                                    container=utils_container)
        else:
            collect_input_out_exec = Transformation("collect.py",
                                                    site=self.target,
                                                    pfn="/usr/local/bin/collect.py",
                                                    is_stageable=False,
                                                    container=utils_container)

        # create an executable for generating a public-private key pair
        if tc_cfg is not None:
            gen_keypair_exec = Transformation("gen_keypair",
                                              site=self.target,
                                              pfn="/usr/local/bin/gen-keypair.py",
                                              is_stageable=False,
                                              arch=Arch.X86_64,
                                              os_type=OS.LINUX,
                                              os_release=tc_cfg['os_release'],
                                              os_version=tc_cfg['os_version'],
                                              container=utils_container)
        else:
            gen_keypair_exec = Transformation("gen_keypair",
                                              site=self.target,
                                              pfn="/usr/local/bin/gen-keypair.py",
                                              is_stageable=False,
                                              container=utils_container)

        self.tc.add_transformations(merge_filter_out_exec,collect_input_out_exec,gen_keypair_exec)

        # executables that create connector and processor plugin subdax
        # path to executable is determined by running "which"
        # executables are "bin" scripts installed via engine package
        conn_plugin_builder_cfg = self.helper.find_exec_path("build-conn-plugin-subdax",self.target)
        build_conn_plugin_subdax = Transformation("build_conn_plugin_subdax",
                                                  site="local",
                                                  is_stageable=False,
                                                  pfn=conn_plugin_builder_cfg['exec_path'])
        build_conn_plugin_subdax.add_profiles(Namespace.ENV,PYTHONPATH=conn_plugin_builder_cfg['python_path'])

        proc_plugin_builder_cfg = self.helper.find_exec_path("build-proc-plugin-subdax",self.target)
        build_proc_plugin_subdax = Transformation("build_proc_plugin_subdax",
                                                  site="local",
                                                  is_stageable=False,
                                                  pfn=proc_plugin_builder_cfg['exec_path'])
        build_proc_plugin_subdax.add_profiles(Namespace.ENV,PYTHONPATH=proc_plugin_builder_cfg['python_path'])

        # executable to build final subdax that simply returns outputs
        final_builder_cfg = self.helper.find_exec_path("build-final-subdax",self.target)
        build_final_subdax = Transformation("build_final_subdax",
                                            site="local",
                                            is_stageable=False,
                                            pfn=final_builder_cfg['exec_path'])
        build_final_subdax.add_profiles(Namespace.ENV,PYTHONPATH=final_builder_cfg['python_path'])

        self.tc.add_transformations(build_conn_plugin_subdax,build_proc_plugin_subdax,build_final_subdax)

        # create an executable for making directories for each workflow stage
        # needs to be at object-level since we need to use the target for the PFN
        if tc_cfg is not None:
            mkdir = Transformation("mkdir",
                                   is_stageable=False,
                                   pfn="/bin/mkdir",
                                   site=self.target,
                                   arch=Arch.X86_64,
                                   os_type=OS.LINUX,
                                   os_release=tc_cfg['os_release'],
                                   os_version=tc_cfg['os_version'])
        else:
            mkdir = Transformation("mkdir",
                                   is_stageable=False,
                                   pfn="/bin/mkdir",
                                   site=self.target,
                                   arch=Arch.X86_64,
                                   os_type=OS.LINUX)

        # executable for final job that moves files to running dir so they can be returned
        if tc_cfg is not None:
            move = Transformation("move",
                                  is_stageable=False,
                                  pfn="/bin/mv",
                                  site=self.target,
                                  arch=Arch.X86_64,
                                  os_type=OS.LINUX,
                                  os_release=tc_cfg['os_release'],
                                  os_version=tc_cfg['os_version'])
        else:
            move = Transformation("move",
                                  is_stageable=False,
                                  pfn="/bin/mv",
                                  site=self.target,
                                  arch=Arch.X86_64,
                                  os_type=OS.LINUX)
        
        self.tc.add_transformations(mkdir,move)

        # add connector and processor plugin containers to DAX-level TC
        # add the corresponding executable for each plguin

        # in dev mode first add all local Singularity containers
        # a local container overrides the same plugin found in the registry
        # this is useful when testing changes to a pre-existing plugin

        local_images = []

        if self.mode == 'dev':
            # find all images in /images
            for file in os.listdir("/images"):
                if file.endswith(".sif"):
                    image_path = os.path.join("/images",file)
                    # figure out if connector or processor
                    plugin_name = os.path.splitext(file)[0]
                    # add to array that is used to identify which images to skip from registry
                    local_images.append(plugin_name.lower())
                    if plugin_name.endswith("Input") or plugin_name.endswith("Filter") or plugin_name.endswith("Output"):
                        #connector
                        exec_name = "run-connector-plugin-%s" % plugin_name.lower()
                    else: # processor
                        exec_name = "run-processor-plugin-%s" % plugin_name.lower()
                        
                    plugin_container = Container(plugin_name.lower(),
                                               Container.SINGULARITY,
                                               image="file://%s" % image_path,
                                               image_site="local",
                                               mounts=["%s:/data/%s" % (self.job_dir,self.workflow_id)])

                    if tc_cfg is not None:
                        plugin_exec = Transformation(exec_name,
                                                   is_stageable=False,
                                                   arch=Arch.X86_64,
                                                   os_type=OS.LINUX,
                                                   os_release=tc_cfg['os_release'],
                                                   os_version=tc_cfg['os_version'],
                                                   site=self.target,
                                                   pfn="/usr/local/bin/run-workflow-stage.sh",
                                                   container=plugin_container)
                    else:
                        plugin_exec = Transformation(exec_name,
                                                   is_stageable=False,
                                                   site=self.target,
                                                   pfn="/usr/local/bin/run-workflow-stage.sh",
                                                   container=plugin_container)
                    
                    self.tc.add_containers(plugin_container)
                    self.tc.add_transformations(plugin_exec)

        # retrieve dictionaries of containers from Singularity registry
        (reg_connectors,reg_processors) = self.helper.get_registry_containers()

        # create container and executable for each connector and processor plugin
        # that isn't a local image
        for conn_plugin in reg_connectors:
            plugin_name = conn_plugin

            if plugin_name in local_images:
               continue

            plugin_image = "library://%s" % reg_connectors[conn_plugin]
            exec_name = "run-connector-plugin-%s" % plugin_name
            
            conn_container = Container(plugin_name,
                                       Container.SINGULARITY,
                                       image=plugin_image,
                                       mounts=["%s:/data/%s" % (self.job_dir,self.workflow_id)])

            self.tc.add_containers(conn_container)

            if tc_cfg is not None:
                conn_exec = Transformation(exec_name,
                                           is_stageable=False,
                                           site=self.target,
                                           arch=Arch.X86_64,
                                           os_type=OS.LINUX,
                                           os_release=tc_cfg['os_release'],
                                           os_version=tc_cfg['os_version'],
                                           pfn="/usr/local/bin/run-workflow-stage.sh",
                                           container=conn_container)
            else:
                conn_exec = Transformation(exec_name,
                                           is_stageable=False,
                                           site=self.target,
                                           pfn="/usr/local/bin/run-workflow-stage.sh",
                                           container=conn_container)
            
            self.tc.add_transformations(conn_exec)

        for proc_plugin in reg_processors:
            plugin_name = proc_plugin

            if plugin_name in local_images:
               continue

            plugin_image = "library://%s" % reg_processors[proc_plugin]
            exec_name = "run-processor-plugin-%s" % plugin_name
            
            proc_container = Container(plugin_name,
                                       Container.SINGULARITY,
                                       image=plugin_image,
                                       mounts=["%s:/data/%s" % (self.job_dir,self.workflow_id)])

            self.tc.add_containers(proc_container)

            if tc_cfg is not None:
                proc_exec = Transformation(exec_name,
                                           is_stageable=False,
                                           site=self.target,
                                           arch=Arch.X86_64,
                                           os_type=OS.LINUX,
                                           os_release=tc_cfg['os_release'],
                                           os_version=tc_cfg['os_version'],
                                           pfn="/usr/local/bin/run-workflow-stage.sh",
                                           container=proc_container)
            else:
                proc_exec = Transformation(exec_name,
                                           is_stageable=False,
                                           site=self.target,
                                           pfn="/usr/local/bin/run-workflow-stage.sh",
                                           container=proc_container)
            
            self.tc.add_transformations(proc_exec)

        self.tc.write('%s/transformations.yml' % self.run_dir)
            
    # build replica catalog
    def build_replica_catalog(self):
        self.rc = ReplicaCatalog()
        tmp_file = File("gpg.txt")
        self.rc.add_replica("local",tmp_file,"/tmp/gpg.txt")

        # we don't write rc yet since we need to add other files

    # initialize Pegasus DAX with an admin task
    # e.g. job for creating the remote data directory for the workflow
    # also creates the local run directory for this workflow
    def init_workflow(self):

        # get a unique identifier based on epoch time
        self.workflow_id = self.helper.gen_workflow_id()
        
        # initialize Workflow
        self.geoedf_wf = Workflow("geoedf-%s" % self.workflow_id)

        # determine full path of new directory to hold intermediate results on target site
        self.job_dir = self.helper.target_job_dir(self.target)

        # create a local run directory; this will be used to store subdax YAMLs and intermediate
        # outputs
        # these will just be workflow outputs and we rely on Pegasus to establish linkages
        # in production mode, this will be used to store the workflow and TC and receive outputs
        # from submit
        self.run_dir = self.helper.create_run_dir()

        # build the transformation catalog
        self.build_transformation_catalog()

        # build replica catalog (turning off since we don't have a need for a RC)
        # self.build_replica_catalog()

        # leaf job used to manage dependencies across stages
        self.leaf_job = None

    # build a Pegasus DAX using the workflow-dict
    def build_pegasus_dax(self):

        # determine number of stages & loop through them creating sub-workflow for each plugin in each stage

        num_stages = len(self.workflow_dict)

        # create a initial job for making a jobdir
        make_workflow_data_dir = Job("mkdir")
        make_workflow_data_dir.add_args("-p",self.job_dir)
        self.geoedf_wf.add_jobs(make_workflow_data_dir)

        # create a job for generating a new public-private key pair on the target site
        # provide workflow_id as keypair filename prefix
        gen_keypair_job = Job("gen_keypair")
        gen_keypair_job.add_args(self.job_dir)

        # set the output file so the public key is returned here for future encryption
        public_key_filename = 'public.pem'
        self.pubkey_file = File(public_key_filename)
        gen_keypair_job.add_outputs(self.pubkey_file)
        
        self.geoedf_wf.add_jobs(gen_keypair_job)

        # add a dependency
        self.geoedf_wf.add_dependency(gen_keypair_job,parents=[make_workflow_data_dir])

        self.leaf_job = gen_keypair_job

        for curr_stage in range(1,num_stages+1):

            stage_data_dir = "%s/%d" % (self.job_dir,curr_stage)
            
            make_stage_data_dir = Job("mkdir")
            make_stage_data_dir.add_args("-p",stage_data_dir)
            self.geoedf_wf.add_jobs(make_stage_data_dir)

            # add a dependency to prior stage
            if self.leaf_job is not None:
                self.geoedf_wf.add_dependency(make_stage_data_dir,parents=[self.leaf_job])

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
                                
                subdax_filename = '%s.yml' % stage_name

                # add subdax file to DAX
                subdax_file = File(subdax_filename)
                #subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
                #self.rc.add_replica("local",subdax_file,subdax_filepath)

                # construct subdax for this plugin, then create a job to execute this subdax
                try:
                    # job constructing subdax for this stage
                    # stage reference and var values files are needed as input
                    dep_vars_str = self.helper.list_to_str(conn_inst.var_dependencies[plugin_id])
                    stage_refs_str = self.helper.list_to_str(conn_inst.stage_refs[plugin_id])

                    dep_var_files = []
                    stage_ref_files = []

                    # create files for the results of the dependent vars and referenced stages
                    for dep_var in conn_inst.var_dependencies[plugin_id]:
                        dep_var_file = File('results_%d_%s.txt' % (stage_num,dep_var))
                        dep_var_files.append(dep_var_file)

                    for stage_ref in conn_inst.stage_refs[plugin_id]:
                        stage_ref_file = File('results_%s.txt' % stage_ref)
                        stage_ref_files.append(stage_ref_file) 

                    # if local args dictionary is empty
                    if not conn_inst.local_file_args[plugin_id]:
                        local_file_args_str = 'None'
                    else:
                        local_file_args_str = json.dumps(json.dumps(conn_inst.local_file_args[plugin_id]))
                    # stage refs with dir modifiers
                    dir_mod_refs_str = self.helper.list_to_str(conn_inst.dir_modified_refs[plugin_id])
                    
                    stage_id = '%d' % stage_num
                    if ':' in plugin_id:
                        res_filename = 'results_%d_%s.txt' % (stage_num,plugin_id.split(':')[1])
                    else:
                        res_filename = 'results_%d.txt' % stage_num
                    stage_res_file = File(res_filename)

                    plugin_name = conn_inst.plugin_names[plugin_id]
                    subdax_job = self.construct_plugin_subdax(stage_id, subdax_filename, plugin_id, plugin_name, dep_vars_str, stage_refs_str,local_file_args_str, sensitive_arg_binds_str, dir_mod_refs_str, dep_var_files, stage_ref_files)
                    subdax_job.add_outputs(subdax_file,stage_out=False,register_replica=False)
                    self.geoedf_wf.add_jobs(subdax_job)

                    # add dependencies; mkdir job and any var dependencies
                    self.geoedf_wf.add_dependency(subdax_job,parents=[stage_mkdir_job])
                                
                    # dependencies on filters
                    for dep_plugin_id in conn_inst.plugin_dependencies[plugin_id]:
                        self.geoedf_wf.add_dependency(subdax_job,parents=[plugin_jobs[dep_plugin_id]])

                    # add job executing sub-workflow to DAX
                    subdax_exec_job = SubWorkflow(subdax_file, is_planned=False)
                    #output_dir = '%s/output' % self.run_dir

                    subdax_exec_job.add_args("-Dpegasus.integrity.checking=none",
                                             "--sites",self.target,
                                             "--output-site","local",
                                             "--basename",stage_name,
                                             "--force")
                    subdax_exec_job.add_outputs(stage_res_file,stage_out=False,register_replica=False)
                    self.geoedf_wf.add_jobs(subdax_exec_job)
                    # add dependency between job building subdax and job executing it
                    self.geoedf_wf.add_dependency(subdax_exec_job,parents=[subdax_job])

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
        proc_inst = GeoEDFProcessor(workflow_stage,stage_num)

        # file to hold the subdax for this plugin
        stage_name = 'stage-%d-Processor' % stage_num
                                
        subdax_filename = '%s.yml' % stage_name

        # add subdax file to DAX
        subdax_file = File(subdax_filename)
        #subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
        #self.rc.add_replica("local",subdax_file,subdax_filepath)

        # construct subdax for this processor plugin, then create a job to execute this subdax
        try:
            # job constructing subdax for this stage
            # stage references are needed as input
            stage_refs_str = self.helper.list_to_str(proc_inst.stage_refs)

            stage_ref_files = []

            # create files for the results of the referenced stages
            for stage_ref in proc_inst.stage_refs:
                stage_ref_file = File('results_%s.txt' % stage_ref)
                stage_ref_files.append(stage_ref_file) 

            # if local args dictionary is empty
            if not proc_inst.local_file_args:
                local_file_args_str = 'None'
            else:
                local_file_args_str = json.dumps(json.dumps(proc_inst.local_file_args))
            
            stage_id = '%d' % stage_num
            plugin_name = proc_inst.plugin_name

            stage_res_file = File('results_%s.txt' % stage_id)

            # if this plugin has any sensitive args, prompt the user for values
            # gets a JSON back
            plugin_sensitive_args = proc_inst.sensitive_args
            if len(plugin_sensitive_args) > 0:
                sensitive_arg_binds = self.helper.collect_sensitive_arg_binds(stage_num,plugin_name,plugin_sensitive_args)
                sensitive_arg_binds_str = json.dumps(json.dumps(sensitive_arg_binds))
            else:
                sensitive_arg_binds_str = 'None'
                
            # args with dir modifiers
            dir_mod_refs_str = self.helper.list_to_str(proc_inst.dir_modified_refs)

            subdax_job = self.construct_plugin_subdax(stage_id, subdax_filename, plugin_name=plugin_name, stage_refs_str=stage_refs_str, local_file_args_str=local_file_args_str, sensitive_arg_binds_str=sensitive_arg_binds_str, dir_mod_refs_str = dir_mod_refs_str, stage_ref_files=stage_ref_files)
            subdax_job.add_outputs(subdax_file,stage_out=False,register_replica=False)
            self.geoedf_wf.add_jobs(subdax_job)

            # add dependency on mkdir job
            self.geoedf_wf.add_dependency(subdax_job,parents=[stage_mkdir_job])
                                
            # add job executing sub-workflow to DAX
            subdax_exec_job = SubWorkflow(subdax_file, is_planned=False)
            #output_dir = '%s/output' % self.run_dir

            subdax_exec_job.add_args("-Dpegasus.integrity.checking=none",
                                     "--sites",self.target,
                                     "--output-site","local",
                                     "--basename",stage_name,
                                     "--force")
            subdax_exec_job.add_outputs(stage_res_file,stage_out=False,register_replica=False)
            self.geoedf_wf.add_jobs(subdax_exec_job)
            # add dependency between job building subdax and job executing it
            self.geoedf_wf.add_dependency(subdax_exec_job,parents=[subdax_job])

            # update the leaf of the DAX to point to this job
            self.leaf_job = subdax_exec_job

        except Exception as e:
            raise GeoEDFError("Error constructing sub-workflow for stage %d: %s" % (stage_num,e))

    # constructs the subdax and its executor job for a final job that returns outputs
    def construct_final_subdax(self,num_stages):

        num_stages_str = '%d' % num_stages
        
        subdax_filename = 'final.yml'

        # add subdax file to DAX
        subdax_file = File(subdax_filename)
        #subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
        #self.rc.add_replica("local",subdax_file, subdax_filepath)

        # results file for last stage
        final_stage_res_file = File('results_%d.txt' % num_stages)

        # construct subdax for this final job, then create a job to execute this subdax
        try:
            # job constructing final subdax
            subdax_build_job = Job("build_final_subdax")
            subdax_build_job.add_selector_profile(execution_site="local")

            # job arguments:
            # num stages
            # subdax filepath
            # remote job directory
            # run directory
            # previous stage results file
            subdax_build_job.add_args(num_stages_str,subdax_filename,self.job_dir,final_stage_res_file)
            subdax_build_job.add_inputs(final_stage_res_file)
            subdax_build_job.add_outputs(subdax_file,stage_out=False,register_replica=False)
            self.geoedf_wf.add_jobs(subdax_build_job)
            
            # add dependency on current leaf job
            self.geoedf_wf.add_dependency(subdax_build_job,parents=[self.leaf_job])
                                
            # add job executing sub-workflow to DAX
            subdax_exec_job = SubWorkflow(subdax_filename, is_planned=False)
            #output_dir = '%s/output' % self.run_dir

            subdax_exec_job.add_args("-Dpegasus.integrity.checking=none",
                                     "--sites",self.target,
                                     "--output-site","local",
                                     "--basename","final")
            self.geoedf_wf.add_jobs(subdax_exec_job)
            # add dependency between job building subdax and job executing it
            self.geoedf_wf.add_dependency(subdax_exec_job,parents=[subdax_build_job])

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
    # stage references that have dir modifiers applied to them in some binding
    # result files for dependent variables
    # result files for referenced stages
    def construct_plugin_subdax(self,workflow_stage,subdax_filepath,plugin_id=None, plugin_name=None, var_deps_str=None,stage_refs_str=None,local_file_args_str=None,sensitive_arg_binds_str=None,dir_mod_refs_str=None,dep_var_files=None,stage_ref_files=None):

        # construct subdax and save in subdax file
        # executable to be used differs based on whether we are
        # converting a connector or processor instance
        if plugin_id is not None: # connector
            subdax_build_job = Job("build_conn_plugin_subdax")
            # always run this job locally
            subdax_build_job.add_selector_profile(execution_site="local")

	    # make the public key an input to this job
            subdax_build_job.add_inputs(self.pubkey_file)

            # job arguments:
            # workflow filepath
            # stage identifier
            # plugin identifier
            # plugin name (used to build executable name)
            # subdax filepath
            # remote job directory
            # run directory
            # dependant vars as comma separated string
            # stage references as comma separated string
            # arg bindings to local files as JSON string
            # stage references that have some dir modifier applied to them
            # public key file
            # dep var result files (as many as dep vars)
            # stage ref result files (as many as stage refs)
            subdax_build_job.add_args(self.workflow_filename,workflow_stage,plugin_id,plugin_name,subdax_filepath,self.job_dir,var_deps_str,stage_refs_str,local_file_args_str,sensitive_arg_binds_str,dir_mod_refs_str,self.pubkey_file)
            
            if dep_var_files is not None:
                for dep_var_file in dep_var_files:
                    subdax_build_job.add_args(dep_var_file)
                    subdax_build_job.add_inputs(dep_var_file)
            
            if stage_ref_files is not None:
                for stage_ref_file in stage_ref_files:
                    subdax_build_job.add_args(stage_ref_file)
                    subdax_build_job.add_inputs(stage_ref_file)
            
            return subdax_build_job

        else:
            subdax_build_job = Job("build_proc_plugin_subdax")

            # always run this job locally
            subdax_build_job.add_selector_profile(execution_site="local")

            # job arguments:
            # workflow filepath
            # stage identifier
            # plugin name (used to build executable name)
            # subdax filepath
            # remote job directory
            # run directory
            # dependant vars as comma separated string
            # stage references as comma separated string
            # arg bindings to local files as JSON string
            # stage references that have some dir modifier applied to them
            # public key file
            # stage ref result files (as many as stage refs)
            subdax_build_job.add_args(self.workflow_filename,workflow_stage,plugin_name,subdax_filepath,self.job_dir,stage_refs_str,local_file_args_str,sensitive_arg_binds_str,dir_mod_refs_str,self.pubkey_file)
            
            if stage_ref_files is not None:
                for stage_ref_file in stage_ref_files:
                    subdax_build_job.add_args(stage_ref_file)
                    subdax_build_job.add_inputs(stage_ref_file)
            
            return subdax_build_job

            
            

