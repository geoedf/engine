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
            self.workflow_dict = yaml.load(workflow_file)
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
        # geoedf_container = Container("geoedf-connector",type="singularity",image="shub://rkalyanapurdue/geoedf-connector:latest",mount=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        geoedf_container = Container("geoedf-connector",type="singularity",image="library://rkalyana/default/geoedf",mount=["%s:/data/%s" % (self.job_dir,self.workflow_id)])
        self.tc.add_container(geoedf_container)
        
        # create and add executables

        conn_exec = Executable("run-workflow-stage",installed=True,container=geoedf_container)
        conn_exec.addPFN(PFN("/usr/local/bin/run-workflow-stage.sh",self.target))
        self.tc.add(conn_exec)

        merge_exec = Executable("merge.py",installed=True,container=geoedf_container)
        merge_exec.addPFN(PFN("/usr/local/bin/merge.py",self.target))
        self.tc.add(merge_exec)

        collect_exec = Executable("collect.py",installed=True,container=geoedf_container)
        collect_exec.addPFN(PFN("/usr/local/bin/collect.py",self.target))
        self.tc.add(collect_exec)

        # executables that create connector and processor plugin subdax
        # path to executable is determined by running "which"
        # executables are "bin" scripts installed via engine package
        build_conn_plugin_subdax = Executable(name="build_conn_plugin_subdax", arch="x86_64", installed=False)
        conn_plugin_builder_path = self.helper.find_exec_path("build-conn-plugin-subdax")
        build_conn_plugin_subdax.addPFN(PFN("file://%s" % conn_plugin_builder_path,"local"))

        self.tc.add(build_conn_plugin_subdax)
        #self.tc.addExecutable(WorkflowBuilder.build_proc_plugin_subdax)

        # create an executable for making directories for each workflow stage
        # needs to be at object-level since we need to use the target for the PFN
        mkdir = Executable(name="mkdir", arch="x86_64", installed=True)
        mkdir.addPFN(PFN("/bin/mkdir",self.target))
        self.tc.add(mkdir)

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

            # naive implementation of connector | processor check
            if 'Input' in workflow_stage.keys():
                stage_type = WorkflowBuilder.CONNECTOR
            else:
                stage_type = WorkflowBuilder.PROCESSOR

            # slightly more complex in case of connector
            if stage_type == WorkflowBuilder.CONNECTOR:

                # instantiate GeoEDFConnector object which performs validation
                # and identifies dependency chain
                conn_inst = GeoEDFConnector(workflow_stage)
                
                # then loop through plugins, creating subdax for any that are fully
                # bound, until all plugins have been run
                plugins = conn_inst.plugin_dependencies.keys()

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
                        if fully_bound:
                            # plugin can be executed, create a subdax for this plugin
                            plugins_done.append(plugin_id)

                            # file to hold the subdax for this plugin
                            # if filter
                            if ':' in plugin_id:
                                stage_name = 'stage-%d-Filter-%s' % (curr_stage,plugin_id.split(':')[1])
                            else: #input
                                stage_name = 'stage-%d-Input' % curr_stage
                                
                            subdax_filename = '%s.xml' % stage_name

                            # add subdax file to DAX
                            subdax_file = File(subdax_filename)
                            subdax_filepath = '%s/%s' % (self.run_dir, subdax_filename)
                            subdax_file.addPFN(PFN("file://%s" % subdax_filepath, "local"))
                            self.dax.addFile(subdax_file)

                            # if subdax construction succeeds, then create a job to execute this subdax
                            try:
                                # job constructing subdax for this stage
                                # stage reference and var values files are needed as input
                                dep_vars_str = self.helper.list_to_str(conn_inst.var_dependencies[plugin_id])
                                stage_refs_str = self.helper.list_to_str(conn_inst.stage_refs[plugin_id])
                                stage_id = '%d' % curr_stage
                                plugin_name = conn_inst.plugin_names[plugin_id]
                                subdax_job = self.construct_plugin_subdax(stage_id, subdax_filepath, plugin_id, plugin_name, dep_vars_str, stage_refs_str)
                                self.dax.addJob(subdax_job)

                                # add dependencies; mkdir job and any var dependencies
                                self.dax.depends(parent=make_stage_data_dir,child=subdax_job)
                                
                                # dependencies on filters
                                for dep_plugin_id in conn_inst.plugin_dependencies[plugin_id]:
                                    self.dax.depends(parent=plugin_jobs[dep_plugin_id],child=subdax_job)

                                # add job executing sub-workflow to DAX
                                subdax_exec_job = DAX(subdax_filename)
                                subdax_exec_job.addArguments("-Dpegasus.catalog.site.file=/usr/local/data/sites.xml",
                                                             "-Dpegasus.integrity.checking=none",
                                                             "--sites",self.target,
                                                             "--output-site","local",
                                                             "--basename",stage_name) #,
                                                             #"--force",
                                                             #"cleanup","none")
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
                        

    # return job constructing subdax for a given workflow stage plugin
    # needs the workflow file, stage number, plugin ID (in case of connectors)
    # file to store subdax XML in
    # remote job directory prefix
    # target site
    # local run directory (to obtain prior stage output files)
    # var dependencies encoded as comma separated string
    # stage references encoded as comma separated string
    def construct_plugin_subdax(self,workflow_stage,subdax_filepath,plugin_id=None, plugin_name=None, var_deps_str=None,stage_refs_str=None):

        # construct subdax and save in subdax file
        # executable to be used differs based on whether we are
        # converting a connector or processor instance
        if plugin_id is not None: # connector
            subdax_build_job = Job(name="build_conn_plugin_subdax")
            # always run this job locally
            subdax_build_job.addProfile(Profile("hints", "execution.site", "local"))

            # for each of the dep vars and stages, add their result files to DAX and as job input
            var_files = []
            stage_files = []
            
            #if var_deps_str != 'None':
                #for var in var_deps_str.split(','):
                #    var_filename = 'results_%s_%s.txt' % (workflow_stage,var)
                #    var_file = File(var_filename)
                #    var_files.append(var_file)
                    
            if stage_refs_str != 'None':
                for stage_ref in stage_refs_str.split(','):
                    stage_filename = 'results_%s.txt' % (workflow_stage,stage_ref)
                    stage_file = File(stage_filename)
                    stage_files.append(stage_file)

            # job arguments:
            # workflow filepath
            # stage identifier
            # plugin identifier
            # subdax filepath
            # remote job directory
            # run directory
            # dependant vars as comma separated string
            # stage references as comma separated string
            # target site
            subdax_build_job.addArguments(self.workflow_filename,workflow_stage,plugin_id,plugin_name,subdax_filepath,self.job_dir,self.run_dir,var_deps_str,stage_refs_str,self.target)
            #for var_file in var_files:
            #    subdax_build_job.addArguments(var_file)
            #    subdax_build_job.uses(var_file, link=Link.INPUT)
            for stage_file in stage_files:
                #subdax_build_job.addArguments(stage_file)
                subdax_build_job.uses(stage_file, link=Link.INPUT)
            return subdax_build_job

        #else:
        #    subdax_build_job = Job(name="build_proc_plugin_subdax")

        
        
                
        
