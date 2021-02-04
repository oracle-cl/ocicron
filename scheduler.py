import os
import sys

from tinydb import TinyDB, Query


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
            "time":"2021-02-04T20:00:0" 
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
    


