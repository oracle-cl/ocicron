#!/usr/bin/python3
import os
import oci
import sys
import json
import argparse
from tinydb import TinyDB, Query
from crontab import CronTab


DEFAULT_LOCATION=os.getcwd()
DEFAULT_PYTHON_ENV=os.path.join(DEFAULT_LOCATION, '.env', 'bin/python')
DB_FILE_NAME="scheduleDB.json"
TAG_KEYS={"Stop", "Start", "Weekend_stop"}
REGIONS=['us-ashburn-1']
COMPARTMENTS=["ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa"]
DEFAULT_AUTH_TYPE='principal'
DEFAULT_PROFILE="DEFAULT"
DEFAULT_SYNC_SCHEDULE='0 23 1 * *'
DEFAULT_SYNC_COMMAND=DEFAULT_PYTHON_ENV + ' ' + 'ocicron.py sync'
CRONTAB_FILE_NAME='ocicron'
CRONTAB_LOCATION='/etc/cron.d'

class OCI:

    def __init__(self, auth_type, config_file="~/.oci/config", profile="DEFAULT", region=None):
        self.auth_type = auth_type
        self.config_file = config_file
        self.profile = profile
        self.region = region

        if self.auth_type == "principal":
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            if self.region is not None:
                self.compute = oci.core.ComputeClient(config={'region':self.region}, signer=signer)
                self.identity = oci.identity.IdentityClient(config={'region':self.region}, signer=signer)
            else:
                self.compute = oci.core.ComputeClient(config={}, signer=signer)
                self.identity = oci.identity.IdentityClient(config={}, signer=signer)
        elif self.auth_type == "config":
            self.config = oci.config.from_file(file_location=config_file, profile_name=profile)
            if self.region is not None:
                self.config['region'] = self.region
            self.compute = oci.core.ComputeClient(self.config)
            self.identity = oci.identity.IdentityClient(self.config)
        else:
            raise Exception("Unrecognize authentication type: auth_type=(principal|config)")
        
        self.compartment_ids = []
        self.compute_instances = []
    
    def _get_sub_compartment_ids(self, cid):
        
        if cid not in self.compartment_ids:
            self.compartment_ids.append(cid)

        result = self.identity.list_compartments(cid)
        if len(result.data) == 0:
            return self.compartment_ids
        else:
            for compartment in result.data:
                if compartment.lifecycle_state == "ACTIVE" and compartment.id not in self.compartment_ids:
                        self.compartment_ids.append(compartment.id)

    def compartment_crawler(self, parent_cid):
        
        self._get_sub_compartment_ids(parent_cid)
        for cid in self.compartment_ids:
            self._get_sub_compartment_ids(cid)
        return self.compartment_ids

    def get_all_instances(self):
        """
        Return all instances in a given compartment
        """
        if len(self.compartment_ids) <= 0:
            return

        for compartment_id in self.compartment_ids:   
            response = self.compute.list_instances(
                compartment_id=compartment_id,
                sort_by="TIMECREATED",
                sort_order="ASC"
            )
            #list instances with pagination
            vms = response.data
            while response.has_next_page:
                response = self.compute.list_instances(compartment_id, page=response.next_page)
                vms.extend(response.data)  

            self.compute_instances.extend(vms)         
        return self.compute_instances
    
    def filter_by_tags(self, tags):
        """
        returns list if OCID of a given tags
        tags = {"Stop":"20","Start": "08","Weekly_stop":"Yes"}
        """
        vmOCID=[]
        for vm in self.compute_instances:
            #compare dictionary and length should be the same
            if len(tags.items() & vm.freeform_tags.items()) == len(tags.items()):
                vmOCID.append(vm.id)
        return vmOCID
    
    def _discover_tags(self, tag_keys=TAG_KEYS):
        """
        Discovery tag keys and values from compute freeform_tags

        example: discover_tag({"Stop", "Start", "Weekly_stop"})
        result: [{'Start': '08', 'Stop': '20', 'Weekly_stop': 'No'}, {'Start': '08', 'Stop': '20', 'Weekly_stop': 'Yes'}]
        """
        result = []
        vm_group = {}
        for vm in self.compute_instances:
            if len(tag_keys & vm.freeform_tags.keys()) == len(tag_keys):
                tags = {}
                for key in tag_keys:
                    tags[key] = vm.freeform_tags[key]
                if tags not in result:
                    result.append(tags)
        return result


    def vms_by_tags(self, tag_keys=TAG_KEYS):   

        tags = self._discover_tags()
        result = []
        for tag in tags:
            vm_group = {}
            vm_group["tags"] = tag
            vm_group["vmOCID"] = self.filter_by_tags(tag)
            result.append(vm_group)
        return result

    def instance_action(self, instances, action):
        """
        Perform a given intance action of a given list of VM OCID
        """
        for ocid in instances:
            self.compute.instance_action(ocid, action)

class ScheduleDB:

    def __init__(self, location=os.path.join(DEFAULT_LOCATION, DB_FILE_NAME)):
        self.location = location
        self.db = TinyDB(self.location)
        self.vm_table = self.db.table('vms')
        self.cid_table = self.db.table('compartments')
        self.cron_table = self.db.table('cron')

        #Query
        self.query = Query()

class Schedule:

    def __init__(self, tabfile=None):
        if tabfile is not None:
            self.tabfile = tabfile
            self.cron = CronTab(user=True, tabfile=self.tabfile)
        else:
            self.cron = CronTab(user=True)
    
    def new(self, command, schedule, comment=None):
        job = self.cron.new(command=command, comment=comment)
        
        job.setall(schedule)
        return self.cron.write()
    
    @staticmethod
    def cron_generator(hour, weekend, region, action):
        """
        EJ: 0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop yes
        r['Stop'], False, region, 'stop'
        """
        #if weekend is True means should remains stopped all weekend
        if weekend == 'yes':
            return '0 {} * * 1-5'.format(hour), '{} ocicron.py --region {} --action {} --at {} --weekend-stop {}'.format(DEFAULT_PYTHON_ENV, region, action, hour, weekend)
        else:
            return '0 {} * * *'.format(hour), '{} ocicron.py --region {} --action {} --at {} --weekend-stop {}'.format(DEFAULT_PYTHON_ENV,region, action, hour, weekend)
    
    def is_schedule(self, schedule):
        """
        Find if a given schedule exists in crontab file
        """
        for job in self.cron.find_time(schedule):
            if job:
                return job
        return None
    def clean_jobs(self, command):
        """
        Find commands in crontab and remove them
        """
        self.cron.remove_all(command=command)
        self.cron.write()

#init function
def init(comparments_ids=COMPARTMENTS, regions=REGIONS):

    db_file = os.path.join(DEFAULT_LOCATION, DB_FILE_NAME)
    if os.path.isfile(db_file):
        print("File {} exists".format(DB_FILE_NAME))
        sys.exit(0)
    db = ScheduleDB()
    oci1 = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE)   

    #crawl compartments
    for cid in comparments_ids:
        oci1.compartment_crawler(cid)
    #Insert compartments in database
    db.cid_table.insert({'compartments': oci1.compartment_ids})
    
    
    for region in regions:
        conn = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE, region=region)
        #No need to search compartments again
        conn.compartment_ids = db.cid_table.all()[0]['compartments']
        #get all instances
        conn.get_all_instances()
        filter_vms = conn.vms_by_tags()
        for vms in filter_vms:
            entry = {
                'region':region,
                'Start':vms['tags']['Start'],
                'Stop':vms['tags']['Stop'],
                'Weekend_stop':vms['tags']['Weekend_stop'],
                'vmOCID':vms['vmOCID']
            }
            db.vm_table.insert(entry)
    
    #schedule jobs
    cronfile = os.path.join(CRONTAB_LOCATION, CRONTAB_FILE_NAME)
    cron = Schedule(cronfile)

    #schedule sync command
    cron.new(DEFAULT_SYNC_COMMAND, DEFAULT_SYNC_SCHEDULE)

    #Loop over regions to fund records and create cronjobs
    for region in regions:
        result = db.vm_table.search(db.query.region==region)
        for r in result:
            if r['Weekend_stop'] == 'No':
                schedule, command = cron.cron_generator(r['Stop'], 'no', region, 'stop')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
                schedule, command = cron.cron_generator(r['Start'], 'no', region, 'start')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
            else:
                schedule, command = cron.cron_generator(r['Stop'], 'yes', region, 'stop')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
                schedule, command = cron.cron_generator(r['Start'], 'yes', region, 'start')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)

def execute(region, action, hour, weekend_stop, **kwargs):
    """
    This function will read argmuments and will find in local database to execute according

    0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop yes
    """
    db = ScheduleDB()
    
    if action == 'stop':
        result = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Stop == hour))
        action = 'SOFTSTOP'
    
    elif action == 'start':
        result = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Start == hour))
        action = 'START'
    else:
        raise Exception("unrecognize action (stop|start)")
    
    if len(result) == 0:
        return "No result found for given query"

    #print(result)
    #connect to OCI
    conn = OCI(auth_type=DEFAULT_AUTH_TYPE, 
        profile=DEFAULT_PROFILE, 
        region=region)

    #given a list of ocid execute action
    conn.instance_action(result[0]['vmOCID'], action)


def sync(comparments_ids=COMPARTMENTS, regions=REGIONS):
    """
    This function will crawl compartments and vms tags and update database and crons if needed 
    """
    db = ScheduleDB()
    oci1 = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE)   

    #crawl compartments
    for cid in comparments_ids:
        oci1.compartment_crawler(cid)
    #check if compartments hasn't change
    if len(db.cid_table.search(db.query.compartments == oci1.compartment_ids)) == 0:
    #Insert compartments in database
        db.cid_table.update({'compartments': oci1.compartment_ids})

       
    for region in regions:
        conn = OCI(DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE, region=region)
        #No need to search compartments again
        conn.compartment_ids = db.cid_table.all()[0]['compartments']
        #get all instances
        conn.get_all_instances()
        filter_vms = conn.vms_by_tags()
        #clean records in table
        db.vm_table.remove(db.query.region==region)
        #Insert records
        for vms in filter_vms:
            entry = {
                'region':region,
                'Start':vms['tags']['Start'],
                'Stop':vms['tags']['Stop'],
                'Weekend_stop':vms['tags']['Weekend_stop'],
                'vmOCID':vms['vmOCID']
            }
            db.vm_table.insert(entry)
    
    #schedule jobs
    cronfile = os.path.join(CRONTAB_LOCATION, CRONTAB_FILE_NAME)
    cron = Schedule(cronfile)
    #clean jobs
    cron.clean_jobs('ocicron.py --region')

    for region in regions:
        result = db.vm_table.search(db.query.region==region)
        for r in result:
            if r['Weekend_stop'] == 'No':
                schedule, command = cron.cron_generator(r['Stop'], 'no', region, 'stop')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
                schedule, command = cron.cron_generator(r['Start'], 'no', region, 'start')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
            else:
                schedule, command = cron.cron_generator(r['Stop'], 'yes', region, 'stop')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
                schedule, command = cron.cron_generator(r['Start'], 'yes', region, 'start')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)

def cli():
    """

    #stop instances in ashburn region where weekly is set to 'yes'
    0 20 * * 1-5 python ocicron.py --region us-ashburn-1 --action stop --at 20 --weekend-stop

    #start instances in ashburn region where Weekend_stop is set to 'no'
    python ocicron.py --region us-ashburn-1 --action starts --at 19
    """
    parser = argparse.ArgumentParser(
        prog='python ocicron.py',
        description='''OCI actions schedule tool. \n
            ocicron was desing to scan freeform_tags in OCI and schedule \n
            start or stop in vms instances. \n

            Use python ocicron.py init to make the first scan. \n
            ''')
    parser.add_argument('--region', help='oci region to connect', required=True)
    parser.add_argument('--action', help='start or stop', choices=['stop', 'start'], required=True)
    parser.add_argument('--at', required=True)
    parser.add_argument('--weekend-stop', help='is this machines should remain stopped on weekends', choices=['yes', 'no'], required=True)

    if sys.argv[1] == 'help':
        parser.print_help()
        sys.exit(0)
    
    if sys.argv[1] == 'init':
        init()
        sys.exit(0)

    if sys.argv[1] == 'sync':
        sync()
        sys.exit(0)

    return parser.parse_args()
 
 

if __name__ == "__main__":
    
    args = cli()
    execute(args.region, args.action, args.at, args.weekend_stop)



    



   
