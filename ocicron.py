#!/usr/bin/python3
import os
import oci
import sys
import json

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
    
    def filter_vms(self, tag_key, tag_value):
        """
        filter VMs OCID of a given filter dictionary
        """
        vmOCID = []
        
        for vm in self.compute_instances:
            if not vm.freeform_tags:
                pass
            else:
                if tag_key in vm.freeform_tags and vm.freeform_tags[tag_key] == tag_value:
                    vmOCID.append(vm.id)
        return {tag_key: tag_value,"vmOCID":vmOCID}
    
    def experimental_filter(self, tags):
        """
        tags = {"stop":"20","start": "80",weekly":"yes"}
        """
        vmOCID=[]
        for vm in self.compute_instances:
            if not vm.freeform_tags:
                pass
            else:
                for key in tags:
                    print(key)
                    if tags[key] != vm.freeform_tags[key]:
                        break
                vmOCID.append(vm.id)
        return vmOCID


    @staticmethod
    def unique(listA,listB):
        return list(set(listA) & set(listB))

    def instance_action(self, instances, action):
        """
        Perform a given intance action of a given list of VM OCID
        """
        for ocid in instances:
            self.compute.instance_action(ocid, action)

class ScheduleDB:

    def __init__(self, location=None):
        self.location = location
        self.db = TinyDB(self.location)

        #stop table
        self.stop_table = self.db.table('stop')
        #start table
        self.start_table = self.db.table('start')
        #weekly table
        #self.weekly_table = self.db.table('weekly')
        #Query
        self.query = Query()
        """
        sample table entry
        {
            "vmOCID": [
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm6pna",
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm6pdfg",
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm62rg"
            ]
            "time":"20",
            "weekly":"yes"
            "region":"us-ashburn-1"
        }
        """

    def _check_entry(self, entry):
        # if 'compartment_id' not in entry:
        #     raise Exception("compartment_id is required")

        if 'vmOCID' not in entry or len(entry['vmOCID']) <= 0:
            raise Exception("At least one  vmOCID is required")

        if 'time' not in entry:
            raise Exception("time field is required")
    
    def insert_stop(self, entry):
        self._check_entry(entry)

        return self.stop_table.insert(entry)

    def insert_start(self, entry):
        self._check_entry(entry)
        return self.start_table.insert(entry)

    # def insert_weekly(self, entry):
    #     self._check_entry(entry)
    #     return self.weekly_table.insert(entry)

    def get_stop_entries(self, compartment_id):
        return self.stop_table.search(self.query.compartment_id == compartment_id)
    
    def get_start_entries(self, compartment_id):
        return self.start_table.search(self.query.compartment_id == compartment_id)
    
    

class Schedule:

    def __init__(self, cronfile=None):
        if cronfile is not None:
            self.cronfile = cronfile
            self.cron = CronTab(user=True, tabfile=self.cronfile)
        else:
            self.cron = CronTab(user=True)

    def add_weekday(self, command, hour, comment=None):

        job = self.cron.new(command=command, comment=comment)
        job.setall('* */{} * * 1-5'.format(hour))
        return self.cron.write()
    
    def add_everyday(self, command, hour, comment=None):
        
        job = self.cron.new(command=command, comment=comment)
        job.setall('* */{} * * * *'.format(hour))
        return self.cron.write()

    def remove_all(self):

        self.cron.remove_all()
        self.cron.write()
    
    def find_remove(self, command):

        iter = self.cron.find_command(command)
        for job in iter:
            self.cron.remove(job)
        self.cron.write()

#init function
def init(comparments_ids, regions):

    db_file = os.path.join(DB_LOCATION_PATH, DB_FILE_NAME)
    if os.path.isfile(db_file):
        print("File {} exists".format(DB_FILE_NAME))
        sys.exit(0)
    profile="ladmcrs"
    ocicrondb = ScheduleDB(db_file)
    
    oci1 = OCI("config", profile=profile)
    

    #crawl compartments
    for cid in comparments_ids:
        oci1.compartment_crawler(cid)
    
    for region in regions:
        conn = OCI("config", profile=profile, region=region)
        #No need to search compartments again
        conn.compartment_ids = oci1.compartment_ids
        #Get all instances
        conn.get_all_instances()
        stop=conn.filter_vms("stop", "20")
        start=conn.filter_vms("start", "08")
        weekly=conn.filter_vms("weekly", "yes")
        noweekly=conn.filter_vms("weekly", "no")
        stop_weekly = conn.unique(stop['vmOCID'], weekly['vmOCID'])
        start_weekly = conn.unique(start['vmOCID'], weekly['vmOCID'])
        stop_noweekly = conn.unique(stop['vmOCID'], noweekly['vmOCID'])
        start_noweekly = conn.unique(start['vmOCID'], noweekly['vmOCID'])
        
        #record in database
        entry = {
            "time":"20",
            "weekly": "yes",
            "region": region,
            "vmOCID": stop_weekly
        }
        ocicrondb.insert_stop(entry)
        entry = {
            "time":"08",
            "weekly": "yes",
            "region": region,
            "vmOCID": start_weekly
        }
        ocicrondb.insert_start(entry)

        entry = {
            "time":"20",
            "weekly": "no",
            "region": region,
            "vmOCID": stop_noweekly
        }
        ocicrondb.insert_stop(entry)
        entry = {
            "time":"08",
            "weekly": "no",
            "region": region,
            "vmOCID": start_noweekly
        }
        ocicrondb.insert_start(entry)

if __name__ == "__main__":

    DB_LOCATION_PATH=os.getcwd()
    DB_FILE_NAME="scheduleDB"

    compartments = ["ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa"]
    all_regions = ["us-ashburn-1"]

   
