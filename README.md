# OCICRON

ocicron is an utitly tool to schedule OCI actions to start or stop compute and database nodes

## Requirements

- Oracle Cloud Infrastructure account
- A VM on any region with minimum specs 1 OCPU, 4GB RAM 
- python >= 3.6
- pip3 >= 9.0.3

## Installation

### Download project and install dependencies
```
git clone https://github.com/davejfranco/ocicron.git
cd ocicron && pip3 install -r requirements.txt
```
### Grant access via Policy
- Create a dynamic group matching the VM OCID
Example:
```
All {instance.id = 'ocid1.instance.oc1.iad.anuwcljsvoaa5zic4qqe56piu4av5dmg3sy5b37hr3vibrwjabaikh6l3ypa'}	
```
If you don't know hoy how to create one refer to this link: [https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/managingdynamicgroups.htm](https://) 

- Create a policy in the root compartment pointing to the dynamic group recently created with the following statements:

	```
	- Allow dynamic-group [dynamic group name] to read all-resources in tenancy where request.operation = 'ListCompartments'	
	- Allow dynamic-group [dynamic group name] to manage instance in tenancy where any {request.operation = 'InstanceAction', request.operation = 'ListInstances'}
	- Allow dynamic-group  [dynamic group name] to manage db-systems in tenancy where any {request.operation = 'ListDbSystems', request.operation = 'GetDbSystem'}		
	- Allow dynamic-group  [dynamic group name] to manage db-nodes in tenancy	
	```
	
Refer to following link on how to create and manage policies on Oracle Cloud [https://docs.oracle.com/en-us/iaas/Content/Identity/Concepts/policygetstarted.htm](https://)

## How to use it 

### step 1
The first thing you need to do is edit the ocicron.py with the regions you need to inspect and if you like you could also limit the compartments aswell. 

### step 2
Execute initialization. This will create the ocicron database and will populate the user's cron with the schedule acording to the tags Star, Stop, Weekend_stop

```
python ocicron.py init
```
### step 3 (Optional)
If you know there is a change you could also sync oci with the ocicron database and cron

```
python ocicron.py sync
```

Cheers ;-)