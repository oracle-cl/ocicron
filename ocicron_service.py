import os
import oci
import logging
import time
from tinydb import TinyDB, Query
from crontab import CronTab


DEFAULT_LOCATION=os.getcwd()
DB_FILE_NAME="scheduleDB.json"
TAG_KEYS={"Stop", "Start", "Weekend_stop"}

#Logging
logging.basicConfig(filename='ocicron.log', level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')

#Fix Too Many request error
custom_retry_strategy = oci.retry.RetryStrategyBuilder(
    # Whether to enable a check that we don't exceed a certain number of attempts
    max_attempts_check=True,
    # check that will retry on connection errors, timeouts and service errors 
    service_error_check=True,
    # a check that we don't exceed a certain amount of time retrying
    total_elapsed_time_check=True,
    # maximum number of attempts
    max_attempts=10,
    # don't exceed a total of 900 seconds for all calls
    total_elapsed_time_seconds=900,
    # if we are checking o service errors, we can configure what HTTP statuses to retry on
    # and optionally whether the textual code (e.g. TooManyRequests) matches a given value
    service_error_retry_config={
        400: ['QuotaExceeded', 'LimitExceeded'],
        429: []
    },
    # whether to retry on HTTP 5xx errors
    service_error_retry_on_any_5xx=True,
    # Used for exponention backoff with jitter
    retry_base_sleep_time_seconds=2,
    # Wait 60 seconds between attempts
    retry_max_wait_between_calls_seconds=60,
    # the type of backoff
    # Accepted values are: BACKOFF_FULL_JITTER_VALUE, BACKOFF_EQUAL_JITTER_VALUE, BACKOFF_FULL_JITTER_EQUAL_ON_THROTTLE_VALUE
    backoff_type=oci.retry.BACKOFF_FULL_JITTER_EQUAL_ON_THROTTLE_VALUE
).get_retry_strategy()


class OCI:

    def __init__(self, auth_type, config_file="~/.oci/config", profile="DEFAULT", region=None):
        self.auth_type = auth_type
        self.config_file = config_file
        self.profile = profile
        self.region = region

        if self.auth_type == "principal":
            self.signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            if region is not None:
                config = {'region':self.region}
            else:
                config = {}            
            self.compute = oci.core.ComputeClient(config=config, signer=self.signer, retry_strategy=custom_retry_strategy)
            self.identity = oci.identity.IdentityClient(config=config, signer=self.signer, retry_strategy=custom_retry_strategy)
            self.database = oci.database.DatabaseClient(config=config, signer=self.signer, retry_strategy=custom_retry_strategy)
        
        elif self.auth_type == "config":
            self.config = oci.config.from_file(file_location=config_file, profile_name=profile)
            if self.region is not None:
                self.config['region'] = self.region
            self.compute = oci.core.ComputeClient(self.config, retry_strategy=custom_retry_strategy)
            self.identity = oci.identity.IdentityClient(self.config, retry_strategy=custom_retry_strategy)
            self.database = oci.database.DatabaseClient(self.config, retry_strategy=custom_retry_strategy)
        
        else:
            logging.exception("Unrecognize authentication type: auth_type=(principal|config)")
            
        
        self.suscribed_regions = []
        self.compartment_ids = []
        self.compute_instances = []
        self.db_systems = []
        self.db_nodes = []
    
    def get_suscribed_regions(self):

        if self.auth_type == "config":
            response = self.identity.list_region_subscriptions(self.config['tenancy'])
        else:
            response = self.identity.list_region_subscriptions(self.signer.tenancy_id)
        
        for r in response.data:
            self.suscribed_regions.append(r.region_name)
        
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

    def compartment_crawler(self, comparments_id=None):

        if comparments_id is not None:
            self._get_sub_compartment_ids(comparments_id)
        else:        
            if self.auth_type == "config":
                self._get_sub_compartment_ids(self.config['tenancy'])
            else:
                self._get_sub_compartment_ids(self.signer.tenancy_id)
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

            for vm in vms:
                if vm.lifecycle_state == 'RUNNING' or vm.lifecycle_state == 'STOPPED':
                    self.compute_instances.append(vm)         
        return self.compute_instances
    
    #Return list of OCID of a given tag, key combination
    def filter_by_tags(self, tags, service='compute'):
        """
        returns list if OCID of a given tags
        tags = {"Stop":"20","Start": "08","Weekly_stop":"Yes"}
        """
        OCIDS=[]
        if service == 'compute':
            for vm in self.compute_instances:
                #compare dictionary and length should be the same
                if len(tags.items() & vm.freeform_tags.items()) == len(tags.items()):
                    OCIDS.append(vm.id)
                    logging.info("VM agregada:" + vm.display_name + " id: " + vm.id)
                else:
                    logging.info("VM descartada:" + vm.display_name)
        elif service == 'database':
            for db in self.db_systems:
                if len(tags.items() & db.freeform_tags.items()) == len(tags.items()):
                    OCIDS.append({"compartment_id": db.compartment_id, "ocid":db.id})
                    logging.info("DB agregada:" + db.display_name + " id: " + db.id)
                else:
                    logging.info("DB descartada:" + db.display_name)
        else:
            logging.error("Unrecognize service: either compute or database are acccepted")
        
        return OCIDS
    
    # Discover all posible tag combinations found on freeform_tags
    def _discover_tags(self, tag_keys=TAG_KEYS, service='compute'):
        """
        example: discover_tag({"Stop", "Start", "Weekly_stop"})
        result: [{'Start': '08', 'Stop': '20', 'Weekly_stop': 'No'}, {'Start': '08', 'Stop': '21', 'Weekly_stop': 'Yes'}]
        """
        result = []
        if service == 'compute':
            for vm in self.compute_instances:
                if len(tag_keys & vm.freeform_tags.keys()) == len(tag_keys):
                    tags = {}
                    for key in tag_keys:
                        tags[key] = vm.freeform_tags[key]
                    if tags not in result:
                        result.append(tags)
        elif service == 'database':
            for db in self.db_systems:
                if len(tag_keys & db.freeform_tags.keys()) == len(tag_keys):
                    tags = {}
                    for key in tag_keys:
                        tags[key] = db.freeform_tags[key]
                    if tags not in result:
                        result.append(tags)
        else:
            logging.error("Unrecognize service: either compute or database are acccepted")

        return result

    #return VMs OCIDs from al tags found
    def vms_by_tags(self):   

        tags = self._discover_tags()
        result = []
        for tag in tags:
            vm_group = {}
            vm_group["tags"] = tag
            vm_group["vmOCID"] = self.filter_by_tags(tag)
            result.append(vm_group)
        return result

    def instance_action(self, instance_ids, action):
        """
        Perform a given intance action of a given list of VM OCID
        """
        if len(instance_ids) <= 0:
            logging.info("No instances IDs")
            return
        for ocid in instance_ids:
            try:     
                logging.info("Try to : {} - instance OCID: {}".format(action, ocid))
                self.compute.instance_action(ocid, action)
                time.sleep(1)
            except Exception as err:
                logging.error("Unable to perform action: {} - instance OCID: {} - Error: {}".format(action, ocid, err))

    #Database service methods
    def get_all_dbsystems(self):
        """
        Return all dbsystems in a given compartment
        """
        if len(self.compartment_ids) <= 0:
            return

        for compartment_id in self.compartment_ids:   
            response = self.database.list_db_systems(
                compartment_id=compartment_id,
                sort_by="TIMECREATED",
                sort_order="ASC"
            )
            #list databse system with pagination
            dbsys = response.data
            while response.has_next_page:
                response = self.database.list_db_systems(compartment_id, page=response.next_page)
                dbsys.extend(response.data)  

            #Store Database system ids
            for dbs in dbsys:
                if dbs.lifecycle_state == 'AVAILABLE':
                    self.db_systems.append(dbs)    
                         
        return self.db_systems
    

    def get_db_nodes(self, compartment_id, db_system_id):
        """
        Return DB Nodes in a given compartment and db system
        """
        response = self.database.list_db_nodes(
            compartment_id=compartment_id,
            db_system_id=db_system_id)        
        return response.data
    
    def dbs_by_tags(self):   

        tags = self._discover_tags(service='database')
        result = []
        for tag in tags:
            db_group = {}
            db_group["tags"] = tag
            for db in self.filter_by_tags(tag, service='database'):
                db_group["dbnodeOCID"] = [ node.id for node in self.get_db_nodes(db["compartment_id"], db["ocid"])]
            result.append(db_group)
        return result

    def database_action(self, db_node_ids, action):
        """
        Perform action of a given list of db nodes OCID
        """
        if len(db_node_ids) <= 0:
            return
        for ocid in db_node_ids:
            try:
                logging.info("Try to : {} - db_node OCID: {}".format(action, ocid))
                self.database.db_node_action(ocid, action)
                time.sleep(1)
            except Exception as err:
                logging.error("Unable to perform action: {} - database OCID: {} - Error: {}".format(action, ocid, err))

class ScheduleDB:

    def __init__(self, location=os.path.join(DEFAULT_LOCATION, DB_FILE_NAME)):
        self.location = location
        self.db = TinyDB(self.location)
        self.vm_table = self.db.table('vms')
        self.dbsys_table = self.db.table('db')
        self.cid_table = self.db.table('compartments')
        self.cron_table = self.db.table('cron')

        #Query
        self.query = Query()


    def flush(self):
        return self.db.drop_table('vms') and self.db.drop_table('db')


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
        self.cron.write()
    
    @staticmethod
    def cron_generator(hour, weekend, region, action):
        """
        EJ: 0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop yes
        r['Stop'], False, region, 'stop'
        """
        #if weekend is True means should remains stopped all weekend
        if weekend == 'yes':
            return '0 {} * * 1-5'.format(hour), 'cd {} && ./ocicron.py --region {} --action {} --at {} --weekend-stop {}'.format(DEFAULT_LOCATION, region, action, hour, weekend)
        else:
            return '0 {} * * *'.format(hour), 'cd {} && ./ocicron.py --region {} --action {} --at {} --weekend-stop {}'.format(DEFAULT_LOCATION, region, action, hour, weekend)
    
    def is_schedule(self, command):
        """
        Find if a given schedule exists in crontab file
        """
        cron_commands = []
        for job in self.cron.find_command(command='ocicron.py'):
            cron_commands.append(job.command)
        
        if command in cron_commands:
            return True
        return False

    def clean_jobs(self, command):
        """
        Find commands in crontab and remove them
        """
        self.cron.remove_all(command=command)
        self.cron.write()
