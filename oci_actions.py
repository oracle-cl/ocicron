#!/usr/bin/python3

import oci
import sys
import json


if sys.platform == 'darwin':
    print("I'm darwing")
    config_file="~/.oci/config"
    profile="ladmcrs"
    config = oci.config.from_file(config_file, profile)
    identity = oci.identity.IdentityClient(config)

if sys.platform == 'linux':
    print("I'm linux")
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    identity = oci.identity.IdentityClient(config={}, signer=signer)
    
sub_compartment_ids = []

def get_sub_compartment_ids(cid):
    global sub_compartment_ids

    if cid not in sub_compartment_ids:
        sub_compartment_ids.append(cid)

    result = identity.list_compartments(cid)
    if len(result.data) == 0:
        return sub_compartment_ids
    else:
        for compartment in result.data:
            if compartment.lifecycle_state == "ACTIVE" and compartment.id not in sub_compartment_ids:
                    sub_compartment_ids.append(compartment.id)
    return sub_compartment_ids


def compartment_crawler(parent_cid):
    
    get_sub_compartment_ids(parent_cid)
    for cid in sub_compartment_ids:
        get_sub_compartment_ids(cid)
    return sub_compartment_ids

class Compute:

    def __init__(self, config_file=None, profile=None, region=None):
        if config_file is not None:
            self.profile = profile
            self.config_file = config_file
            self.config = oci.config.from_file(
                file_location=config_file,
                profile_name=self.profile)           
            self.client = oci.core.ComputeClient(self.config)
        else:
            if region is not None:
                self.region = region
                signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
                self.client = oci.core.ComputeClient(config={'region':self.region}, signer=signer)

    def get_instances(self, compartment_id):
        """
        Return all instances in a given compartment
        """
        response = self.client.list_instances(
            compartment_id=compartment_id,
            sort_by="TIMECREATED",
            sort_order="ASC"
        )
        vms = response.data
        while response.has_next_page:
            response = self.client.list_instances(compartment_id, page=response.next_page)
            vms.extend(response.data)           

        return vms

        """ 
        if tag_type == "freeform_tags":
            for vm in instances.data:
                for k in vm.freeform_tags.keys():
                    if k in tag and vm.freeform_tags[k] == tag[k]:
                        valid_vm.append(vm.id)

        if tag_type == "defined_tags":
            for vm in instances.data:
                for k in vm.defined_tags.keys():
                    if k in tag and vm.defined_tags[k] == tag[k]:
                        valid_vm.append(vm.id)
        """    
    @staticmethod    
    def filter_vms(vms, tag_key, tag_value):
        """
        filter VMs OCID of a given filter dictionary
        """
        vmOCID = []
        for vm in vms:
            if not vm.freeform_tags:
                pass
            else:
                if tag_key in vm.freeform_tags and vm.freeform_tags[tag_key] == tag_value:
                    vmOCID.append(vm.id)
        return {tag_key:tag_value, "vmOCID":vmOCID}


    def instance_action(self, instances, action):
        """
        Perform a given intance action of a given list of VM OCID
        """
        for ocid in instances:
            self.client.instance_action(ocid, action)


if __name__ == '__main__':

    cid="ocid1.compartment.oc1..aaaaaaaa4bybtq6axk7odphukoulaqsq6zdewp7kgqunjxhw3icuohglhnwa"
    compartment_crawler(cid)

    vm = Compute(config_file, profile)
    response = vm.get_instances(cid)

    vm_by_cid = {}
    for cid in sub_compartment_ids:
        vm_by_cid[cid] = vm.get_instances(cid)
    
    filtered_vms_by_ocid = {}
    for key in vm_by_cid.keys():
        filtered_vms_by_ocid[key] = vm.filter_vms(vm_by_cid[key], 'weekly', 'yes')
    
    print(filtered_vms_by_ocid)   