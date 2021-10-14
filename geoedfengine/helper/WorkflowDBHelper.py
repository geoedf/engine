#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Implements methods for interacting with a GeoEDF specific workflow DB and 
    the existing Pegasus DBs
"""

import sys
import os
import sqlite3
from .GeoEDFError import GeoEDFError

class WorkflowDBHelper:

    # initialize class object
    # create the db file and geoedf_workflow table if not exist
    def __init__(self):

        # initialize key variables
        self.geoedf_cursor = None
        self.geoedf_con = None
        self.pegasus_cursor = None

        # HUBzero specific; assume workflow DB file is in home directory
        if os.getenv('HOME') is not None:
            # geoedf workflow DB file path
            geoedf_dbfile = '%s/geoedf(DO_NOT_DELETE).db' % os.getenv('HOME')
            try:
                con = sqlite3.connect(geoedf_dbfile)
                con.row_factory = sqlite3.Row
                self.geoedf_con = con
                self.geoedf_cursor = con.cursor()
                self.geoedf_cursor.execute('''CREATE TABLE if not exists geoedf_workflow(wfid INTEGER PRIMARY KEY AUTOINCREMENT, workflow_name TEXT NOT NULL, pegasus_workflow_name TEXT NOT NULL, workflow_rundir TEXT NOT NULL, tool_shortname TEXT NOT NULL, UNIQUE(workflow_name))''')
            except:
                raise GeoEDFError("Error initializing GeoEDF workflow database")

            # Pegasus workflow DB file path
            pegasus_dbfile = '%s/.pegasus/workflow.db' % os.getenv('HOME')
            try:
                con = sqlite3.connect(pegasus_dbfile)
                con.row_factory = sqlite3.Row
                self.pegasus_cursor = con.cursor()
            except:
                raise GeoEDFError("Error initializing Pegasus master workflow database")

        else:
            raise GeoEDFError("Home directory not found; cannot find or initialize GeoEDF workflow database")

    #check to make sure workflow_name is unique
    #we provide a method so as to return a useful error message
    def check_unique_workflow(self,workflow_name):
        if self.geoedf_cursor is not None:
            # check to see if workflow with this name already exists
            validate_querystr = "SELECT * from geoedf_workflow WHERE workflow_name = '%s';" % workflow_name
            validate_res = self.query(self.geoedf_cursor,validate_querystr)
            if len(validate_res) > 0:
                raise GeoEDFError("A workflow with the name '%s' already exists; please choose a different name" % workflow_name)
        else:
            raise GeoEDFError("Cannot execute commands against GeoEDF workflow database!!!")

    #insert record into GeoEDF workflow table
    def insert_workflow(self,workflow_name,pegasus_workflow_name,workflow_rundir,tool_shortname):
        if self.geoedf_cursor is not None:
            # insert workflow record
            self.geoedf_cursor.execute("INSERT OR IGNORE INTO geoedf_workflow(workflow_name, pegasus_workflow_name, workflow_rundir, tool_shortname) VALUES('%s','%s','%s','%s')" % (workflow_name,pegasus_workflow_name,workflow_rundir,tool_shortname))
            self.geoedf_con.commit()
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


    # method to check status of workflow tasks and group them into buckets
    # of complete, executing, and pending tasks
    # determines the build- and run- tasks for each plugin and then queries
    # the status of their corresponding Pegasus jobs
    # needs workflow DB file for this specific workflow
    
    #1 Filter:dtstring
    #2 HDFEOSShapefileMask
    #1 Filter:filename
    #1 Input
    #1:Filter:dtstring Filter
    #1:Filter:filename Filter
    #1:Input Input
    #2 Processor
    def get_workflow_tasks_status(self,workflow_dbfile):

        complete_tasks = []
        pending_tasks = []
        executing_tasks = []
        workflow_complete = False

        try:
            con = sqlite3.connect(workflow_dbfile)
            con.row_factory = sqlite3.Row
            workflow_cursor = con.cursor()

            task_querystr = "SELECT transformation,argv,job_id from task where transformation like 'build_%_plugin_subdax' or transformation like 'run%plugin%';"

            task_jobs = self.query(workflow_cursor,task_querystr)

            num_tasks = len(task_jobs)

            # for each task job, get the job instance ID, then query its states
            for task_job in task_jobs:
                task_jobid = task_job['job_id']
                task_transformation = task_job['transformation']
                task_data = task_job['argv'].split(' ')
                task_stage = task_data[1]
                task_plugin = task_data[2]

                if task_transformation.startswith('build'):
                    task_id = '%s:%s' % (task_stage,task_plugin)
                else: #running tasks
                    if ':' in task_stage:
                        task_id = task_stage
                    else:
                        task_id = '%s:%s' % (task_stage,task_plugin)

                job_instid_querystr = "SELECT job_instance_id from job_instance where job_id = %d;" % int(task_jobid)

                task_jobinst = self.query(workflow_cursor,job_instid_querystr)

                # assuming there only exists one
                if len(task_jobinst) > 0:
                    job_instid = int(task_jobinst[0]['job_instance_id'])

                    # get states
                    job_state_querystr = "SELECT state from jobstate where job_instance_id = %d;" % job_instid

                    job_states_res = self.query(workflow_cursor,job_state_querystr)

                    if len(job_states_res) > 0:
                        # check to see if JOB_SUCCESS and POST_SCRIPT_SUCCESS are present
                        # if so, this task is done, so continue
                        # if not, then this is the one being executed
                        job_states = [row['state'] for row in job_states_res]
                        if 'JOB_SUCCESS' in job_states:
                            complete_tasks.append(task_id)
                        else:
                            #this task is still being worked on
                            executing_tasks.append(task_id)
                    else:
                        pending_tasks.append(task_id)
                else:
                    pending_tasks.append(task_id)

                if num_tasks == len(complete_tasks):
                    workflow_complete = True

            return (complete_tasks,executing_tasks,pending_tasks,workflow_complete)

        except:
            raise GeoEDFError("Exception occurred when trying to determine current workflow task!!!")

    # find earliest task from an array using stage number and type
    # only called if > 0 tasks present
    def identify_earliest_task(self,tasks):
        earliest_task = tasks[0]
        for i in range(1,len(tasks)):
            curr_task = tasks[i]
            earliest_task_stage = int(earliest_task.split(':')[0])
            curr_task_stage = int(curr_task.split(':')[0])
            if curr_task_stage < earliest_task_stage:
                earliest_task = curr_task
            elif curr_task_stage > earliest_task_stage:
                continue
            else: #same stage; has to be a connector; filter comes before input
                earliest_plugin = earliest_task.split(':')[1]
                curr_plugin = curr_task.split(':')[1]
                if earliest_plugin == 'Input' and curr_plugin == 'Filter':
                    earliest_task = curr_task

        # make earliest_task more human readable
        task_data = earliest_task.split(':')
        task_stage = task_data[0]
        task_plugin_type = task_data[1]

        if task_plugin_type == 'Input' or task_plugin_type == 'Filter':
            if len(task_data) > 2:
                filter_var = task_data[2]
                return "Stage %s Filter plugin for variable %s" % (task_stage,filter_var)
            else:
                return "Stage %s Input plugin" % task_stage
        else: #processor plugin
            return "Stage %s Processor plugin" % task_stage

    # based on the various task arrays, figure out most current task
    def get_current_task(self,executing_tasks,pending_tasks):
        if len(executing_tasks) == 0 and len(pending_tasks) == 0:
            # unable to figure out what the current task is
            return None
        else:
            if len(executing_tasks) == 0:
                # there are only pending tasks that haven't begun execution
                earliest_task = self.identify_earliest_task(pending_tasks)
                return "Waiting to execute %s" % earliest_task
            else:
                earliest_task = self.identify_earliest_task(executing_tasks)
                return "Currently executing %s" % earliest_task

    # method to monitor a workflow's progress
    # workflow name is optional; when Null all workflows matching tool_shortname
    # are retrieved
    def get_workflow_status(self,workflow_name=None,tool_shortname=None):
        status_res = []
        rundirs = {}
        pegasus_workflows = {}
        dax_workflownames = {}
        
        if workflow_name is None:
            # query for all workflow names for this tool_shortname
            # if unknown, query all workflows
            if tool_shortname is None:
                get_wf_names_query_str = "SELECT workflow_name,pegasus_workflow_name,workflow_rundir from geoedf_workflow;"
            else:
                get_wf_names_query_str = "SELECT workflow_name,pegasus_workflow_name,workflow_rundir from geoedf_workflow WHERE tool_shortname = '%s';" % tool_shortname
        else: #workflow name has been provided; still need to query for rundir
            if tool_shortname is None:
                get_wf_names_query_str = "SELECT workflow_name,pegasus_workflow_name,workflow_rundir from geoedf_workflow WHERE workflow_name = '%s';" % workflow_name
            else:
                get_wf_names_query_str = "SELECT workflow_name,pegasus_workflow_name,workflow_rundir from geoedf_workflow WHERE tool_shortname = '%s' AND workflow_name = '%s';" % (tool_shortname,workflow_name)

        res = self.query(self.geoedf_cursor,get_wf_names_query_str)
            
        workflow_names = ['%s' % row['workflow_name'] for row in res]

        for row in res:
            rundirs[row['workflow_name']] = row['workflow_rundir']
            pegasus_workflows[row['workflow_name']] = row['pegasus_workflow_name']
            dax_workflownames[row['pegasus_workflow_name']] = row['workflow_name']

        # for each workflow, query the Pegasus master_workflow table to fetch db_url
        # query string is different based on the number of workflow names
        if len(workflow_names) == 0:
            print("No workflows found")
            return status_res
        elif len(workflow_names) == 1:
            get_wf_db_url_query_str = "SELECT dax_label,db_url FROM master_workflow WHERE dax_label in ('%s');" % pegasus_workflows[workflow_names[0]]
        else:
            get_wf_db_url_query_str = "SELECT dax_label,db_url FROM master_workflow WHERE dax_label in %s;" % (tuple([pegasus_workflows[workflow_name] for workflow_name in workflow_names]),)
        res = self.query(self.pegasus_cursor,get_wf_db_url_query_str)

        for row in res:
            # check to see if db_url still points to an existent file
            # on workflow completion, the workflow db file is moved to the top level folder
            # in workflow_dir
            # db_url is of the form: sqlite:///<path>
            dbpath = row['db_url'][10:]
            workflow_dbfname = os.path.split(dbpath)[1]
            rundir = rundirs[dax_workflownames[row['dax_label']]]
            if not os.path.isfile(dbpath):
                # check to see if file can be found in top level workflow dir
                dbpath = '%s/%s' % (rundir,workflow_dbfname)
                if not os.path.isfile(dbpath):
                    print("Workflow %s status cannot be determined; workflow tracking database is missing!!!" % dax_workflownames[row['dax_label']])
                    continue
                else:
                    res_data = {}
                    res_data['workflow_id'] = dax_workflownames[row['dax_label']]
                    res_data['workflow_dir'] = rundir
                    res_data['workflow_status'] = 'Workflow complete'
                    status_res.append(res_data)
            else:
                # query for task status in the workflow_db file
                (complete_tasks, executing_tasks, pending_tasks, workflow_complete) = self.get_workflow_tasks_status(dbpath)

                # if workflow is complete
                if workflow_complete:
                    res_data = {}
                    res_data['workflow_id'] = dax_workflownames[row['dax_label']]
                    res_data['workflow_dir'] = rundir
                    res_data['workflow_status'] = 'Workflow complete'
                    status_res.append(res_data)
                else:
                    # identify most current task for this workflow
                    curr_task = self.get_current_task(executing_tasks,pending_tasks)
                    if curr_task is not None:
                        res_data = {}
                        res_data['workflow_id'] = dax_workflownames[row['dax_label']]
                        res_data['workflow_dir'] = rundir
                        res_data['workflow_status'] = curr_task
                        status_res.append(res_data)
                    else:
                        res_data = {}
                        res_data['workflow_id'] = dax_workflownames[row['dax_label']]
                        res_data['workflow_dir'] = rundir
                        res_data['workflow_status'] = 'Unknown'
                        status_res.append(res_data)

        return status_res
