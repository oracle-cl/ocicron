#!/usr/bin/python3
import os
import oci
import sys
import json
from tinydb import TinyDB, Query
from crontab import CronTab


DEFAULT_LOCATION=os.getcwd()
DB_FILE_NAME="scheduleDB.json"
TAG_KEYS={"Stop", "Start", "Weekend_stop"} 

class OCI:

    def __init__(self, auth_type="principal", config_file="~/.oci/config", profile="DEFAULT", region=None):
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

    def insert_vm(self, entry):
        """
        sample table entry
        {
            "vmOCID": [
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm6pna",
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm6pdfg",
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm62rg"
            ]
            "Stop":"20",
            "Start":"08",
            "Weekly_stop":"Yes"
            "region":"us-ashburn-1"
        }
        """
        return self.vm_table.insert(entry)
    
    def insert_cids(self, entry):
        """
        {
            "compartments": [
                "ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa",
                "ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhasd",
                "ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohg2daty"
            ]
        }
        """
        return self.cid_table.insert(entry)
    
    def find_by_region(self, region):
        return self.vm_table.search(self.query.region==region)



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
    def cron_generator(hour, weekend, region, action, user='opc'):
        """
        EJ: 0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop
        r['Stop'], False, region, 'stop'
        """
        #if weekend is True means should remains stopped all weekend
        if weekend:
            return '0 {} * * 1-5'.format(hour), 'python ocicron.py --region {} --action {} --at {} --weekend-stop'.format(region, action, hour)
        else:
            #the cron will execute everyday at given hour
            return '0 {} * * *'.format(hour), 'python ocicron.py --region {} --action {} --at {}'.format(region, action, hour)

    
    def is_schedule(self, schedule):
        """
        Find if a given schedule exists in crontab file
        """
        for job in self.cron.find_time(schedule):
            if job:
                return job
        return None

    def remove_all(self):
        #remove all jobs in cron file
        self.cron.remove_all()
        self.cron.write()
    
    def remove(self, job):
        #remove job
        self.cron.remove(job)
        self.cron.write()

#init function
def init(comparments_ids, regions):

    db_file = os.path.join(DEFAULT_LOCATION, DB_FILE_NAME)
    if os.path.isfile(db_file):
        print("File {} exists".format(DB_FILE_NAME))
        sys.exit(0)
    db = ScheduleDB()
    profile="ladmcrs"
    oci1 = OCI("config", profile=profile)   

    #crawl compartments
    for cid in comparments_ids:
        oci1.compartment_crawler(cid)
    #Insert compartments in database
    db.insert_cids({'compartments': oci1.compartment_ids})
    
    
    for region in regions:
        conn = OCI("config", profile=profile, region=region)
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
            db.insert_vm(entry)
    
    #schedule jobs
    cronfile = os.path.join(DEFAULT_LOCATION, 'cron.tab')
    cron = Schedule(cronfile)


    for region in regions:
        result = db.find_by_region(region)
        for r in result:
            if r['Weekend_stop'] == 'No':
                schedule, command = cron.cron_generator(r['Stop'], False, region, 'stop')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
                schedule, command = cron.cron_generator(r['Start'], False, region, 'start')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
            else:
                schedule, command = cron.cron_generator(r['Stop'], True, region, 'stop')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)
                schedule, command = cron.cron_generator(r['Start'], True, region, 'start')
                if not cron.is_schedule(schedule):
                    cron.new(command, schedule)

def execute(region, action, hour, weekend_stop, **kwargs):
    """
    This function will read argmuments and will find in local database to execute according

    0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop
    """
    db = ScheduleDB()
    
    if action == 'stop':
        result = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop) & (db.query.Stop == hour))
        action = 'SOFTSTOP'
    
    elif action == 'start':
        result = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop) & (db.query.Start == hour))
        action = 'START'
    else:
        raise Exception("unrecognize action (stop|start)")
    
    if len(result) == 0:
        return "No result found for given query"

    #print(result)
    #connect to OCI
    conn = OCI(auth_type=kwargs['auth_type'], 
        profile=kwargs['profile'], 
        region=region)

    #given a list of ocid execute action
    conn.instance_action(result[0]['vmOCID'], action)


def sync():
    return
def argparser():
    """

    #stop instances in ashburn region where weekly is set to 'yes'
    python ocicron.py --region us-ashburn-1 --action stop --weekly

    #start instances in ashburn region where weekly is set to 'no'
    python ocicron.py --region us-ashburn-1 --action start 
    """

    
    return      
 

if __name__ == "__main__":

    profile="ladmcrs"
    cid="ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa"
    region = 'us-ashburn-1'
    execute(region, 'start', '07', 'No', auth_type='config', profile=profile)



    



   
