#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Implements workflow monitoring for GeoEDF workflows; utilizes both a GeoEDF 
    specific workflow DB and the existing Pegasus DBs for managing and querying status
    For now, only the build and run tasks for each of the workflow plugins are 
    monitored
"""

import sys
import os
import sqlite3
from ..GeoEDFConfig import GeoEDFConfig
from .GeoEDFError import GeoEDFError

class WorkflowMonitor:

    # initialize class object
    # create the db file and geoedf_workflow table if not exist
    # also tries to identify the tool_shortname (HZ only) to enable
    # filtering workflow queries by this specific tool
    def __init__(self):

        # initialize key variables
        self.geoedf_cursor = None
        self.pegasus_cursor = None
        self.tool_shortname = None
        
        if os.getenv('HOME') is not None:
            # geoedf workflow DB file path
            geoedf_dbfile = '%s/geoedf(DO_NOT_DELETE).db' % os.getenv('HOME')
            try:
                con = sqlite3.connect(geoedf_dbfile)
                self.geoedf_cursor = con.cursor()
                self.geoedf_cursor.execute('''CREATE TABLE if not exists geoedf_workflow(wfid INTEGER PRIMARY KEY AUTOINCREMENT, workflow_name TEXT NOT NULL, workflow_rundir TEXT NOT NULL, tool_shortname TEXT NOT NULL, UNIQUE(workflow_name))''')
            except:
                raise GeoEDFError("Error initializing GeoEDF workflow database")

            # Pegasus workflow DB file path
            pegasus_dbfile = '%s/.pegasus/workflow.db' % os.getenv('HOME')
            try:
                con = sqlite3.connect(pegasus_dbfile)
                self.pegasus_cursor = con.cursor()
            except:
                raise GeoEDFError("Error initializing Pegasus master workflow database")

            # determine tool shortname if possible
            if os.getenv('TOOLDIR') is not None:
                tooldir = os.getenv('TOOLDIR')
                if tooldir.startswith('/apps/'):
                    # assuming tooldir is of the form: /apps/tool_shortname/release/
                    portions = tooldir.split('/')
                    if len(portions) > 2:
                        self.tool_shortname = portions[2]
        else:
            raise GeoEDFError("Home directory not found; cannot find workflow database to monitor")

    # start monitoring this GeoEDF workflow
    # essentially insert record into GeoEDF workflow table
    def start_monitor(self,workflow_name,workflow_rundir):
        if self.geoedf_cursor is not None:
            self.geoedf_cursor.execute("INSERT OR IGNORE INTO geoedf_workflow(workflow_name, workflow_rundir, tool_shortname) VALUES(%s,%s,%s)" % (workflow_name,workflow_rundir,self.tool_shortname))
        else:
            raise GeoEDFError("Cannot execute commands against GeoEDF workflow database!!!")

    # method to query a table given a SQLite cursor and return a dict
    def query(self,cursor,query_str):
        try:
            cursor.execute(query_str)
            res = cursor.fetchall()
            data = [dict(row) for row in res]
            return data
        except:
            raise GeoEDFError("Error occurred executing query %s" % query_str)


    # method to check status of workflow tasks and return task being executed currently
    # determines the build- and run- tasks for each plugin and then queries
    # the status of their corresponding Pegasus jobs
    # needs workflow DB file for this specific workflow
    # assumes only one active task exists; i.e. will return first task that has started and is still executing
    
    #1 Filter:dtstring
    #2 HDFEOSShapefileMask
    #1 Filter:filename
    #1 Input
    #1:Filter:dtstring Filter
    #1:Filter:filename Filter
    #1:Input Input
    #2 Processor
    def current_workflow_task(self,workflow_dbfile):

        try:
            con = sqlite3.connect(workflow_dbfile)
            workflow_cursor = con.cursor()

            task_querystr = "SELECT transformation,argv,job_id from task where transformation like 'build_%_plugin_subdax' or transformation like 'run%plugin%';"

            task_jobs = self.query(workflow_cursor,task_querystr)

            # for each task job, get the job instance ID, then query its states
            for task_job in task_jobs:
                task_jobid = task_job['job_id']
                task_transformation = task_job['transformation']
                task_data = task_job['argv'].split(' ')
                task_stage = task_data[1]
                task_plugin = task_data[2]

                if task_transformation.startswith('build'):
                    if ':' in task_plugin: #filter
                        plugin_data = task_plugin.split(':')
                        current_task = 'Building stage %s Filter plugin for variable %s' % (task_stage,plugin_data[1])
                    else:
                        if task_plugin == 'Input':
                            current_task = 'Building stage %s Input plugin' % task_stage
                        else:
                            current_task = 'Building stage %s Processor plugin' % task_stage
                else:
                    if ':' in task_stage:
                        stage_data = task_stage.split(':')
                        stage_num = stage_data[0]
                        if len(stage_data) > 2: #filter
                            filter_val = stage_data[2]
                            current_task = 'Building stage %s Filter plugin for variable %s' % (stage_num,filter_var)
                        else: #input
                            current_task = 'Building stage %s Input plugin' % stage_num
                    else:
                        current_task = 'Building stage %s Processor plugin' % task_stage

                job_instid_querystr = "SELECT job_instance_id from job_instance where job_id = %d;" % int(task_jobid)

                task_jobinst = self.query(workflow_cursor,job_instid_querystr)

                # assuming there only exists one
                if len(task_jobinst) > 0:
                    job_instid = int(task_jobinst[0]['job_instance_id'])

                    # get states
                    job_state_querystr = "SELECT state from jobstate where job_instance_id = %d;" % job_instid

                    job_states_res = self.query(workflow_cursor,job_state_querystr)

                    if len(job_states) > 0:
                        # check to see if JOB_SUCCESS and POST_SCRIPT_SUCCESS are present
                        # if so, this task is done, so continue
                        # if not, then this is the one being executed
                        job_states = [row['state'] for row in job_states_res]
                        if 'JOB_SUCCESS' in job_states:
                            continue
                        else:
                            return current_task
        except:
            raise GeoEDFError("Exception occurred when trying to determine current workflow task!!!")

    # method to monitor a workflow's progress
    # workflow name is optional; when Null all workflows matching tool_shortname
    # are retrieved
    def monitor_status(self,workflow_name=None):
        status_res = {}
        if workflow_name is None:
            # query for all workflow names for this tool_shortname
            # if unknown, query all workflows
            if self.tool_shortname is None:
                get_wf_names_query_str = "SELECT workflow_name from geoedf_workflow;"
            else:
                get_wf_names_query_str = "SELECT workflow_name from geoedf_workflow WHERE tool_shortname = '%s';" % self.tool_shortname
            res = self.query(self.geoedf_cursor,get_wf_names_query_str)
            
            workflow_names = [row['workflow_name'] for row in res]
        else:
            workflow_names = [workflow_name]

        # for each workflow, query the Pegasus master_workflow table to fetch db_url
        get_wf_db_url_query_str = "SELECT dax_label,db_url FROM master_workflow WHERE dax_label in %s;" % tuple(workflow_names)
        res = self.query(self.pegasus_cursor,get_wf_db_url_query_str)

        for row in res:
            # check to see if db_url still points to an existent file
            # on workflow completion, the workflow db file is moved to the top level folder
            # in workflow_dir
            # db_url is of the form: sqlite:///<path>
            dbpath = row['db_url'][10:]
            rundir = row['workflow_rundir']
            if not os.path.isfile(dbpath):
                # check to see if file can be found in top level workflow dir
                dbpath = '%s/workflow.db' % rundir
                if not os.path.isfile(dbpath):
                    print("Workflow %s status cannot be determined; workflow tracking database is missing!!!")
                    continue

            # query for task status in the workflow_db file
            curr_task = self.current_workflow_task(dbpath)
            status_res[row['dax_label']] = curr_task

        return status_res
