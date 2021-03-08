# OCICRON

ocicron is an utitly tool to schedule OCI actions to start or stop compute and database nodes

## Requirements
- python >= 3.6
- pip3 >= 21.0.1
- Oracle Cloud Infrastructure account


## Install

´´´shell
pip3 install -r requirements.txt
´´´

## How to use it 

### step 1
The first thing you need to do is edit the ocicron.py with the regions you need to inspect and if you like you could also limit the compartments aswell. 

### step 2
Execute initialization. This will create the ocicron database and will populate the user's cron with the schedule acording to the tags Star, Stop, Weekend_stop
´´´python
python ocicron.py init
´´´
### step 3 (Optional)
If you know there is a change you could also sync oci with the ocicron database and cron
´´´python
python ocicron.py sync
´´´

Cheers ;-)
