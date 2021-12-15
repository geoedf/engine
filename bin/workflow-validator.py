import yaml
from yaml import FullLoader
import os

#list of valid executable names
valid_exec_names = ['mkdir','gen_keypair','build_conn_plugin_subdax','build_proc_plugin_subdax','build_final_subdax']

#standard app prefix
app_prefix = '/apps/share64'

# function to validate a YAML Pegasus workflow
# ensures that each job in the workflow is drawn
# from a restricted set of executables
# each subworkflow uses a logical file name and
# not a physical file in some local path
def validate_workflow(workflow_filepath):
    # read workflow YAML file
    try:
        with open(workflow_filepath,'r') as workflow_file:
            workflow_dict = yaml.load(workflow_file,Loader=FullLoader)

            # next loop through the jobs in the dictionary
            # process "job" separately from "pegasusworkflow" type
            for workflow_job in workflow_dict['jobs']:
                if workflow_job['type'] == 'job':
                    # check the executable name
                    # transformation catalog will be validated separately
                    # to ensure these execs are not invalid
                    exec_name = workflow_job['name']
                    if exec_name not in valid_exec_names:
                        raise Exception('Invalid executable %s used in workflow' % exec_name)
                elif workflow_job['type'] == 'pegasusWorkflow':
                    # for pegasus workflow jobs (i.e., subworkflow)
                    # ensure file used only has lfn, this means it will be
                    # generated by a subdax construction job
                    subdax_file = workflow_job['file']

                    # if this is a full qualified path, raise error
                    if os.path.dirname(subdax_file) != '':
                        raise Exception('Attempt to override subdax file ',subdax_file)

                    #get file links and check for subdax_file
                    #at this point, subdax_file does not have a leading dir path
                    for link in workflow_job['uses']:
                        if 'lfn' in link:
                            if link['lfn'] == subdax_file and link['type'] == 'input':
                                # check if a pfn is also present
                                if 'pfn' in link:
                                    raise Exception('Attempt to override subdax file ',subdax_file)
                        # check if a pfn entry exists for this file
                        if 'pfn' in link:
                            dir,filename = os.path.split(link['pfn'])
                            if filename == subdax_file:
                                # directory has to be empty
                                if dir != '':
                                    raise Exception('Attempt to override subdax file ',subdax_file)
    except:
        raise Exception('Could not validate workflow file')

# function to validate the transformation catalog
# ensures that any transformations that use local site
# do not use non-standard paths
# also ensure no local containers are used
def validate_transformations(transformation_filepath):
    # read transformations catalog file
    try:
        with open(transformation_filepath,'r') as transforms_file:
            transforms_dict = yaml.load(transforms_file,Loader=FullLoader)

            #first check container to make sure they are not local files
            for exec_container in transforms_dict['containers']:
                #ensure image doesn't begin with 'file:'
                if exec_container['image'].startswith('file://'):
                    raise Exception('Local container used in workflow')
            
            #loop through transformations and check their site
            #only worry about those that do not use a container
            for transformation in transforms_dict['transformations']:
                sites = transformation['sites']
                for site in sites:
                    # if container is supplied, we don't need to worry
                    # this is because we've ensured no local containers are present
                    if 'container' in site:
                        continue
                    else:
                        # check for local site, pfn and the path
                        if site['name'] == 'local' and 'pfn' in site:
                            #mygeohub specific
                            #check if pfn has app prefix, else complain
                            if site['pfn'].startswith(app_prefix):
                                continue
                            else:
                                raise Exception('Non-standard path in transformation ',transformation['name'])
    except:
        raise Exception('Could not validate transformations catalog file')

# main code begins
# first retrieve workflow path from command args
if len(sys.argv) > 1:
    workflow_filepath = sys.argv[1]

    # ensure this is a valid filepath
    if os.path.isfile(workflow_filepath):
        dir,workflow_fname = os.path.split(workflow_filepath)

        # construct path to transformations catalog in same directory
        transformation_filepath = '%s/transformations.yml' % dir

        # first validate workflow
        validate_workflow(workflow_filepath)

        # validate transformations file
        validate_transformations(transformation_filepath)

        exit(1)
    else:
        raise Exception('Invalid workflow file argument provided to workflow-validator script')
else:
    raise Exception('Workflow file path not provided to workflow-validator script')
