import logging
import json
import os
from datetime import datetime, timedelta

class Diff():
    def __init__(self):
        self.data_dict = {}
        self.diff_dict = {}
        self.ASN_peer_dict = {}
        
        self.mlogger = logging.getLogger(f'routeDiff')

    def init(self):
        self.data_dict = {}
        self.diff_dict = {}
        self.ASN_peer_dict = {}
        
        self.mlogger = logging.getLogger(f'routeDiff')
    
    def construct_routing_file(self, save_dir):
        self.mlogger.debug('Start constructing routing file')
        with open(save_dir, "w") as f:
            for ASN in self.data_dict:
                for prefix in self.data_dict[ASN]["to_origin"]:
                    Str = ",".join(self.data_dict[ASN]["to_origin"][prefix])
                    f.write(f'to|{ASN}|{prefix}|{Str}\n')

    
    def construct_routing_by_List(self, infoList, pfx):
        for as_path in infoList:
            if '{' in as_path:
                continue
            as_path_list = as_path.split(' ')
            
            for i in reversed(range(len(as_path_list) - 1)):
                if not as_path_list[i] == as_path_list[-1]:
                    ASN, r = as_path_list[i], as_path_list[-1]
                    if ASN not in self.data_dict:
                        self.data_dict.setdefault(ASN, {"to_origin" : {}})
                    if r not in self.ASN_peer_dict:
                        self.ASN_peer_dict.setdefault(r, set())
                    if pfx not in self.data_dict[ASN]['to_origin']:
                        self.data_dict[ASN]["to_origin"].setdefault(pfx, set())
                    self.data_dict[ASN]["to_origin"][pfx].add(as_path_list[i])
                    self.ASN_peer_dict[r].add(ASN)
                    break
        
        return
    
    def compare_routing(self, old_routing_file):
        logging.debug("Start comparing routing")
        diff, new_dict = {}, self.data_dict
        with open(old_routing_file, "r") as f:
            for line in f:
                if "{" in line:
                    continue
                rtype, ASN, prefix, to_origin_peers = line.replace('\n', '').split('|')
                to_origin_peers = set(to_origin_peers.split(','))
                diff.setdefault(ASN, {"to_origin": [], "summary": {"to_origin_increase": 0, "to_origin_decrease": 0, "to_origin_change": 0}})
                old_str = ",".join(to_origin_peers)
                if (not ASN in new_dict) or (not prefix in new_dict[ASN]["to_origin"]):
                    diff[ASN]["to_origin"].append(f'-|{prefix}|{old_str}')
                    diff[ASN]["summary"]["to_origin_decrease"] += 1
                elif not to_origin_peers == new_dict[ASN]["to_origin"][prefix]:
                    new_str = ",".join(new_dict[ASN]["to_origin"][prefix])
                    diff[ASN]["to_origin"].append(f'-|{prefix}|{old_str}')
                    diff[ASN]["to_origin"].append(f'+|{prefix}|{new_str}')
                    new_dict[ASN]["to_origin"].pop(prefix, None)
                    diff[ASN]["summary"]["to_origin_change"] += 1
                else:
                    new_dict[ASN]["to_origin"].pop(prefix, None)
                    
        for ASN in new_dict:
            for prefix in new_dict[ASN]["to_origin"]:
                new_str = ",".join(new_dict[ASN]["to_origin"][prefix])
                diff.setdefault(ASN, {"to_origin": [], "summary": {"to_origin_increase": 0, "to_origin_decrease": 0, "to_origin_change": 0}})
                diff[ASN]["to_origin"].append(f'+|{prefix}|{new_str}')
                diff[ASN]["summary"]["to_origin_increase"] += 1
        
        return diff

    def convert_set_to_list(self):
        self.mlogger.info("[routeDiff] Converting set to list")
        for ASN in self.data_dict:
            self.data_dict[ASN]['to_origin'] = list(self.pfx_dict[ASN]['to_origin'])
    

if __name__ == '__main__':
    pass