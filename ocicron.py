#!/usr/bin/python3
import os
import sys
import argparse
from ocicron_service import OCI, ScheduleDB, Schedule


DEFAULT_LOCATION=os.getcwd()
DB_FILE_NAME="scheduleDB.json"
TAG_KEYS={"Stop", "Start", "Weekend_stop"}
REGIONS=['us-ashburn-1', 'sa-santiago-1']
COMPARTMENTS=["ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa"]
DEFAULT_AUTH_TYPE='config'
DEFAULT_PROFILE="ladmcrs"
DEFAULT_SYNC_SCHEDULE='0 23 1 * *'
DEFAULT_SYNC_COMMAND=DEFAULT_LOCATION + '/ocicron.py sync'
CRONTAB_FILE_NAME=os.path.join(os.getcwd(),'ocicron.tab')
#CRONTAB_LOCATION='/etc/cron.d'
#CRONTAB_LOCATION=os.getcwd()


def schedule_commands():
    """
    this function will read database and will schedule command execution
    """
    db = ScheduleDB()
    result = db.vm_table.all()
    cron = Schedule(CRONTAB_FILE_NAME)

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
    cron = Schedule(CRONTAB_FILE_NAME)

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
    cron = Schedule(CRONTAB_FILE_NAME)
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
    
    #args = cli()
    #execute(args.region, args.action, args.at, args.weekend_stop)
    schedule_commands()



    



   
