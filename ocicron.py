#!/usr/bin/python3
import os
import sys
import logging
import argparse
from ocicron_service import OCI, ScheduleDB, Schedule


DEFAULT_LOCATION=os.getcwd()
REGIONS=['us-ashburn-1', 'sa-santiago-1']
#COMPARTMENTS=["ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa"]
COMPARTMENTS=[]
DEFAULT_AUTH_TYPE='config'
DEFAULT_PROFILE="DEFAULT"
DEFAULT_SYNC_SCHEDULE='0 23 1 * *'
DEFAULT_SYNC_COMMAND='cd {} && ./ocicron.py sync'.format(DEFAULT_LOCATION)
CRONTAB_FILE_NAME=os.path.join(os.getcwd(),'ocicron.tab')

#ocicron Database
db = ScheduleDB()
#Crontab
cron = Schedule()
#Logging
logging.basicConfig(filename='ocicron.log', level=logging.INFO, format='%(asctime)s :: %(levelname)s :: %(message)s')


def schedule_commands():
    """
    this function will read database and will schedule command execution
    """
    result = db.vm_table.all()

    for r in result:
        if r['Weekend_stop'] == 'No':
            schedule, command = cron.cron_generator(r['Stop'], 'no', r['region'], 'stop')
            if not cron.is_schedule(command):
                cron.new(command, schedule)
            schedule, command = cron.cron_generator(r['Start'], 'no', r['region'], 'start')
            if not cron.is_schedule(command):
                cron.new(command, schedule)
        else:
            schedule, command = cron.cron_generator(r['Stop'], 'yes', r['region'], 'stop')
            if not cron.is_schedule(command):
                cron.new(command, schedule)
            schedule, command = cron.cron_generator(r['Start'], 'yes', r['region'], 'start')
            if not cron.is_schedule(command):
                cron.new(command, schedule)

def generate_entries(regions):

    entries = []
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
            conn.get_all_instances()
        except Exception as e:
            logging.error("Exception occurred", exc_info=True)
            sys.exit()
        filter_vms = conn.vms_by_tags()
        #vm_entries = []
        for vms in filter_vms:
            entry = {
                'region':region,
                'Start':vms['tags']['Start'],
                'Stop':vms['tags']['Stop'],
                'Weekend_stop':vms['tags']['Weekend_stop'],
                'vmOCID':vms['vmOCID']
            }
            entries.append(entry)
    return entries

#init function
def init(comparments_ids=COMPARTMENTS, regions=REGIONS):

    logging.info('ocicron is initiating')
    if len(db.vm_table.all()) > 0 or len(db.cid_table.all()) > 0:
        logging.info('Database already exists')
        sys.exit()
    
    oci1 = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE)   

    if len(COMPARTMENTS) <= 0:
        oci1.compartment_crawler()
    else:
        #crawl compartments
        for cid in comparments_ids:
            oci1.compartment_crawler(cid)

    #Insert compartments in database
    db.cid_table.insert({'compartments': oci1.compartment_ids})
    
    #Scan region and generate entries to the database
    for entry in generate_entries(REGIONS):
        db.vm_table.insert(entry)
    
    #schedule sync command - check this as well
    if not cron.is_schedule(DEFAULT_SYNC_COMMAND):
        cron.new(DEFAULT_SYNC_COMMAND, DEFAULT_SYNC_SCHEDULE)

    #Loop over regions to fund records and create cronjobs
    schedule_commands()
    logging.info('Start/Stop commands has been scheduled')

def vm_execute(region, action, hour, weekend_stop, **kwargs):
    """
    This function will read argmuments and will find in local database to execute according

    0 20 * * * python ocicron.py --region us-ashburn-1 --action stop --at 09 --weekend-stop yes
    """
    
    if action == 'stop':
        result = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Stop == hour))
        action = 'SOFTSTOP'
    
    elif action == 'start':
        result = db.vm_table.search((db.query.region == region) & (db.query.Weekend_stop == weekend_stop.capitalize()) & (db.query.Start == hour))
        action = 'START'
    else:
        raise Exception("unrecognize action (stop|start)")
    
    if len(result) == 0:
        logging.warning('No result found for given query -- region:{}, action:{}, hour{}, weekend_stop: {}'.format(region, action, hour, weekend_stop))
        sys.exit()
    else:
        logging.info('{} VM OCIDs match with query'.format(result[0]['vmOCID']))

    #print(result)
    #connect to OCI
    try:
        conn = OCI(auth_type=DEFAULT_AUTH_TYPE, 
            profile=DEFAULT_PROFILE, 
            region=region)
    except Exception as e:
        logging.error(e, exc_info=True)
        sys.exit()

    #given a list of ocid execute action
    try:
        conn.instance_action(result[0]['vmOCID'], action)
    except Exception as e:
        logging.error("Exception occurred", exc_info=True)
        sys.exit(9)


def sync(comparments_ids=COMPARTMENTS, regions=REGIONS):
    """
    This function will crawl compartments and vms tags and update database and crons if needed 
    """

    logging.info('ocicron is syncing')
    oci1 = OCI(auth_type=DEFAULT_AUTH_TYPE, profile=DEFAULT_PROFILE)   

    if len(COMPARTMENTS) <= 0:
        oci1.compartment_crawler()
    else:
        #crawl compartments
        for cid in comparments_ids:
            oci1.compartment_crawler(cid)
    #check if compartments hasn't change
    if len(db.cid_table.search(db.query.compartments == oci1.compartment_ids)) == 0:
    #Insert compartments in database
        db.cid_table.update({'compartments': oci1.compartment_ids})

    #Scan region and generate entries to the database 
    for entry in generate_entries(REGIONS):
        db.vm_table.insert(entry)
    
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
    
    args = cli()
    vm_execute(args.region, args.action, args.at, args.weekend_stop)



    



   
