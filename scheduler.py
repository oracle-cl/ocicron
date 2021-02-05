import os
import oci
import sys
from crontab import CronTab
from tinydb import TinyDB, Query


#def scan():



class ScheduleDB:

    def __init__(self, location=None):
        self.location = location
        if self.location is not None:
            self.db = TinyDB(self.location)
        else:
            self.location = os.getcwd() + '/' + 'scheduleDB.json'
            self.db = TinyDB(self.location)

        #stop table
        self.stop_table = self.db.table('stop')
        #start table
        self.start_table = self.db.table('start')
        #weekly table
        self.weekly_table = self.db.table('weekly')
        #Query
        self.query = Query()
        """
        sample table entry
        {   "compartment_id": 
                "ocid1.compartment.oc1..aaaaaaaauw5f7a5u2nme6kii66guhb6br6e5n3avkcujwt6o4jwcrietpehq",
            "vmOCID": [
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm6pna",
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm6pdfg",
                "ocid1.instance.oc1.iad.anuwcljsvoaa5zicx2fh2bln35lx6ldtydtnyjfjoq26nwk5q7yozbnm62rg"
            ]
            "time":"20",
            "region":"us-ashburn-1"
        }
        """

    def _check_entry(self, entry):
        if 'compartment_id' not in entry:
            raise Exception("compartment_id is required")

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

    def insert_weekly(self, entry):
        self._check_entry(entry)
        return self.weekly_table.insert(entry)

    def get_stop_entries(self, compartment_id):
        return self.stop_table.search(self.query.compartment_id == compartment_id)
    
    def get_start_entries(self, compartment_id):
        return self.start_table.search(self.query.compartment_id == compartment_id)
    
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


    


