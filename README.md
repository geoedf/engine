# GeoEDF Plug-and-play Workflow Engine

The GeoEDF Workflow Engine transforms an abstract GeoEDF workflow written in _YAML_ syntax into a 
concrete sequence of tasks executed on HPC resources. Under the hood, the engine utilizes the 
Pegasus Workflow Management System (WMS) to plan and manage the execution of these tasks. 

This version of the workflow engine has been designed for installation on HUBzero-based science gateways 
and for use in Jupyter notebook tools running on HUBzero. Rather than utilize the Pegasus WMS directly, 
it uses the HUBzero _submit_ wrapper for managing workflow execution and data transfer between HUBzero tools 
and the execution machines. 

The workflow engine is a Python library, with the **GeoEDFWorkflow** class providing the key workflow 
instantiation and execution functionality. 

In order to plan and execute a GeoEDF YAML workflow run:

```python
    from geoedfengine.GeoEDFWorkflow import GeoEDFWorkflow
    workflow = GeoEDFWorkflow('PATH_TO_WORKFLOW_YML_FILE')
    workflow.execute()
```

Execution is asynchronous; and can be monitored using the workflow directory outputted by the _execute_ step 
above as follows:


```python
    workflow = GeoEDFWorkflow(workflow_dir='WORKFLOW_DIR_REPORTED_BY_EXECUTE')
    workflow.monitor()
```

### Notes:

1. The **geoedf.cfg** file is used to configure the workflow engine by specifying the execution target, the 
   paths to relevant executables, HUBzero submit details, etc. Since this is site-specific, only a barebones
   config file is included here.
