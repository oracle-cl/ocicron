#!/usr/bin/python3
from logging import exception
import os
import sys
import argparse
from ocicron_service import OCI, ScheduleDB, Schedule, logging


DEFAULT_LOCATION=os.getcwd()
COMPARTMENTS=[]
DEFAULT_AUTH_TYPE='principal'
DEFAULT_PROFILE="DEFAULT"
DEFAULT_SYNC_SCHEDULE='0 23 * * *'
DEFAULT_SYNC_COMMAND='cd {} && ./ocicron.py sync'.format(DEFAULT_LOCATION)

#Crontab
cron = Schedule()

#ocicron Database
db = ScheduleDB()

def schedule_commands():
    """
    this function will read database and will schedule command execution
    """
    #Get all entries in the vm table and the db table
    result = db.vm_table.all()
    result.extend(db.dbsys_table.all())

    for r in result:
            schedule, command = cron.cron_generator(r['Stop'], r['Weekend_stop'].lower(), r['region'], 'stop')
            if not cron.is_schedule(command):
                cron.new(command, schedule)
            schedule, command = cron.cron_generator(r['Start'], r['Weekend_stop'].lower(), r['region'], 'start') 
            if not cron.is_schedule(command):
                cron.new(command, schedule)

def generate_entries(regions):

    entries = {}
    vm_entries = []
    dbs_entries = []
    for region in regions:
        try:
            conn = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE, region=region)
        except Exception as e:
            logging.error(e, exc_info=True)
            sys.exit()
        #No need to search compartments again
        conn.compartment_ids = db.cid_table.all()[0]['compartments']
        #get all instances
        try:
            #Get all VMs
            conn.get_all_instances()
            #Get all DB systems
            conn.get_all_dbsystems()
        except Exception as e:
            logging.error("Exception occurred", exc_info=True)
            sys.exit()
        #filtered VMs
        filter_vms = conn.vms_by_tags()
        #filtered DBs
        filter_dbs = conn.dbs_by_tags()
        #Generate entry and collect them
        for vms in filter_vms:
            entry = {
                'region':region,
                'Start':vms['tags']['Start'],
                'Stop':vms['tags']['Stop'],
                'Weekend_stop':vms['tags']['Weekend_stop'],
                'vmOCID':vms['vmOCID']
            }
            vm_entries.append(entry)
        
        for dbs in filter_dbs:
            entry = {
                'region':region,
                'Start':dbs['tags']['Start'],
                'Stop':dbs['tags']['Stop'],
                'Weekend_stop':dbs['tags']['Weekend_stop'],
                'dbnodeOCID':dbs['dbnodeOCID']
            }
            dbs_entries.append(entry)
        
        entries['vms'] = vm_entries
        entries['db_nodes'] = dbs_entries        
    return entries

#init function
def init(comparments_ids=COMPARTMENTS):


    logging.info('ocicron is initiating')
    if len(db.vm_table.all()) > 0 or len(db.cid_table.all()) > 0:
        logging.info('Database already exists')
        sys.exit()
    
    oci = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE)

    #get account suscribe regions
    oci.get_suscribed_regions()

    if len(COMPARTMENTS) <= 0:
        oci.compartment_crawler()
    else:
        #crawl compartments
        for cid in comparments_ids:
            oci.compartment_crawler(cid)

    #Insert compartments in database
    db.cid_table.insert({'compartments': oci.compartment_ids})
    
    #Scan region and generate entries to the database
    entries = generate_entries(oci.suscribed_regions)
    for vm in entries['vms']:
        db.vm_table.insert(vm)
    
    for nodes in entries['db_nodes']:
        db.dbsys_table.insert(nodes)
    
    #schedule sync command - check this as well
    if not cron.is_schedule(DEFAULT_SYNC_COMMAND):
        cron.new(DEFAULT_SYNC_COMMAND, DEFAULT_SYNC_SCHEDULE)

    #Loop over regions to fund records and create cronjobs
    schedule_commands()
    logging.info('Start/Stop commands has been scheduled')

def execute(region, action, hour, weekend_stop, **kwargs):
    """
    This function will read argmuments and will find in local database to execute according

    0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop yes
    """
    
    if action == 'stop':
        vm_query = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Stop == hour))
        dbs_query = db.dbsys_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Stop == hour))  
    elif action == 'start':
        vm_query = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Start == hour))
        dbs_query = db.dbsys_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Start == hour))
    else:
        logging.exception("unrecognize action (stop|start)")

    #connect to OCI
    try:
        conn = OCI(auth_type=DEFAULT_AUTH_TYPE, 
            profile=DEFAULT_PROFILE, 
            region=region)
    except Exception as e:
        logging.error(e, exc_info=True)

    #Compute Service
    if len(vm_query) <= 0:
        logging.warning('No VM resources found for this given query -- region:{}, action:{}, hour:{}, weekend_stop:{}'.format(region, action, hour, weekend_stop))
    else:
        logging.info("Executing {} action in Compute service, in region: {} at: {} and Weekend_stop: {} on {} instances".format(action, region, hour, weekend_stop, len(vm_query)))
        if action == 'stop':
            action = 'softstop'

        #Execute Instance action on returned instances OCID
        conn.instance_action(vm_query[0]['vmOCID'], action.upper())
    
    #Database Service
    if len(dbs_query) <= 0:
        logging.warning('No DB system resources found for this given query -- region:{}, action:{}, hour:{}, weekend_stop:{}'.format(region, action, hour, weekend_stop))
    else:
        try:
            logging.info("Executing {} action in database service, in region: {} at: {} and Weekend_stop: {}".format(action, region, hour, weekend_stop))
            conn.database_action(dbs_query[0]['dbnodeOCID'], action.upper())
        except Exception as e:
            logging.error(e)

#sync command to update entries
def sync(comparments_ids=COMPARTMENTS):
    """
    This function will crawl compartments and vms tags and update database and crons if needed 
    """
    logging.info('ocicron is syncing')

    oci = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE)   
    #get account suscribe regions
    oci.get_suscribed_regions()

    if len(COMPARTMENTS) <= 0:
        oci.compartment_crawler()
    else:
        #crawl compartments
        for cid in comparments_ids:
            oci.compartment_crawler(cid)
    
    #
    try:
        db.flush()
    except Exception as err:
        logging.exception(err)
    
    #call databse table object
    db.vm_table
    db.dbsys_table

    #check if compartments hasn't change
    if len(db.cid_table.search(db.query.compartments == oci.compartment_ids)) == 0:
    #Insert compartments in database
        db.cid_table.update({'compartments': oci.compartment_ids})

    #Scan region and generate entries to the database
    entries = generate_entries(oci.suscribed_regions)
    for vm in entries['vms']:
        db.vm_table.insert(vm)
    
    for nodes in entries['db_nodes']:
        db.dbsys_table.insert(nodes)
    
    #clean jobs
    cron.clean_jobs('ocicron.py --region')
    #query and create cronjobs
    schedule_commands()

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
    
    #argument parser
    args = cli()
    #find and execute action over VMs and DB systems
    execute(args.region, args.action, args.at, args.weekend_stop)




    



   
