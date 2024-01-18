import os
abs_path = os.path.abspath(__file__)
par_path = os.path.split(abs_path)[0]
parpar_path = os.path.split(par_path)[0]
parparpar_path = os.path.split(parpar_path)[0]
par_path += '/'
parpar_path += '/'
parparpar_path += '/'
import sys
sys.path.append(os.path.join(parparpar_path, 'configs'))
sys.path.append(par_path)
sys.path.append(parpar_path)

import IPy
import logging
import json
import pymongo
import os
import ipaddress
from logging.handlers import RotatingFileHandler
from routeDiff.updater import Updater
from datetime import datetime, timedelta
from rconfig import MONGO_DB, IDS, IPv4_Bogon, IPv6_Bogon

handler = RotatingFileHandler(os.path.join(parparpar_path, 'logs/routeDiff-running.log'), encoding='UTF-8', maxBytes=5*1024*1024, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")
handler.setFormatter(formatter)
mlogger = logging.getLogger(f'routeDiff')
mlogger.setLevel(logging.INFO)
mlogger.addHandler(handler)

class DiffExecute():
    def __init__(self, current_date):
        self.db_updater = Updater(MONGO_DB, current_date)
        self.data_dict = {}
        self.diff_dict = {}
        self.ASN_peer_dict = {}
    
    def diff_init(self, current_date):
        self.db_updater = Updater(MONGO_DB, current_date)
        self.data_dict = {}
        self.diff_dict = {}
        self.ASN_peer_dict = {}
    
    def _isBogon(self, pfx):
        # ipv4 bogon check
        if '.' in pfx and ':' not in pfx:
            for bogon in IPv4_Bogon:
                if pfx in IPy.IP(bogon):
                    return True, bogon
            return False, None
        else: # ipv6 bogon check
            for bogon in IPv6_Bogon:
                if pfx in IPy.IP(bogon):
                    return True, bogon
            return False, None

    def filterBogon(self):
        # filter bogon
        for ASN in list(self.data_dict.keys()):
            for prefix in list(self.data_dict[ASN]["to_origin"].keys()):
                flag, bogon = self._isBogon(prefix)
                if flag == True:
                    self.data_dict[ASN]["to_origin"].pop(prefix, None)
            if len(self.data_dict[ASN]["to_origin"]) == 0:
                '''print(self.data_dict[ASN]["to_origin"])
                print(ASN)'''
                self.data_dict.pop(ASN, None)
            
    
    def construct_routing_file(self, save_dir):
        mlogger.info(f'[routeDiff] Start constructing routing file')
        with open(save_dir, "w") as f:
            for ASN in self.data_dict:
                for prefix in self.data_dict[ASN]["to_origin"]:
                    Str = ",".join(self.data_dict[ASN]["to_origin"][prefix])
                    f.write(f'to|{ASN}|{prefix}|{Str}\n')
    
    def _get_full_ipv6_prefix(self, pfx):
        position = pfx.find('/')
        ip = pfx[:position]
        length = pfx[position:]
        return f'{ipaddress.ip_address(ip).exploded}{length}'
    
    def construct_routing_by_file(self, file_dir):
        mlogger.info(f'[routeDiff] handling file {file_dir}')
        line_counter = 0
        with open(file_dir, 'r') as f:
            for line in f:
                items = line.split('|')

                line_counter += 1
                if line_counter % 500000 == 0:
                    mlogger.info(f'[routeDiff] file {file_dir} proccessd {line_counter} lines')
                # not the correct format
                if not len(items) == 15:
                    continue

                _, ts, b, nh, vp, pfx, as_path, origin, nh, _1, _2, community, agg, _3, _4 = items

                if pfx == '0.0.0.0/0' or pfx == '::/0':
                    continue

                if len(as_path.split(' ')) < 2:
                    continue

                # ipv4
                if '.' in pfx and ':' not in pfx:
                    ID = int(hex(int(pfx.split('.')[0]))[2:].zfill(2)[0], 16)
                else: # ipv6
                    ID = int(self._get_full_ipv6_prefix(pfx).split(':')[0][0], 16)
                
                if ID not in IDS:
                    continue

                if '{' in as_path:
                    continue

                as_path_list = as_path.split(' ')

                if len(as_path_list) == 1:
                    ASN, r = '-1', as_path_list[-1]
                    if r not in self.data_dict:
                        self.data_dict.setdefault(r, {"to_origin" : {}})
                    if pfx not in self.data_dict[r]['to_origin']:
                        self.data_dict[r]["to_origin"].setdefault(pfx, set())
                    self.data_dict[r]["to_origin"][pfx].add(ASN)
                    continue

                for i in reversed(range(len(as_path_list) - 1)):
                    if not as_path_list[i] == as_path_list[-1]:
                        ASN, r = as_path_list[i], as_path_list[-1]
                        if r not in self.data_dict:
                            self.data_dict.setdefault(r, {"to_origin" : {}})
                        if r not in self.ASN_peer_dict:
                            self.ASN_peer_dict.setdefault(r, set())
                        if pfx not in self.data_dict[r]['to_origin']:
                            self.data_dict[r]["to_origin"].setdefault(pfx, set())
                        self.data_dict[r]["to_origin"][pfx].add(ASN)
                        self.ASN_peer_dict[r].add(ASN)
                        break
        
        mlogger.info(f'[routeDiff] file {file_dir} finished')
    
    def compare_routing(self, old_routing_file):
        mlogger.info(f'[routeDiff] Start comparing routing')
        diff= {}
        with open(old_routing_file, "r") as f:
            for line in f:
                if "{" in line:
                    continue
                rtype, ASN, prefix, to_origin_peers = line.replace('\n', '').split('|')
                to_origin_peers = set(to_origin_peers.split(','))
                diff.setdefault(ASN, {"to_origin": [], "summary": {"to_origin_increase": 0, "to_origin_decrease": 0, "to_origin_change": 0}})
                old_str = ",".join(to_origin_peers)
                if (not ASN in self.data_dict) or (not prefix in self.data_dict[ASN]["to_origin"]):
                    diff[ASN]["to_origin"].append(f'-|{prefix}|{old_str}')
                    diff[ASN]["summary"]["to_origin_decrease"] += 1
                elif not to_origin_peers == self.data_dict[ASN]["to_origin"][prefix]:
                    #new_str = ",".join(self.data_dict[ASN]["to_origin"][prefix])
                    #diff[ASN]["to_origin"].append(f'-|{prefix}|{old_str}')
                    #diff[ASN]["to_origin"].append(f'+|{prefix}|{new_str}')
                    self.data_dict[ASN]["to_origin"].pop(prefix, None)
                    diff[ASN]["summary"]["to_origin_change"] += 1
                else:
                    self.data_dict[ASN]["to_origin"].pop(prefix, None)
                    
        for ASN in self.data_dict:
            for prefix in self.data_dict[ASN]["to_origin"]:
                new_str = ",".join(self.data_dict[ASN]["to_origin"][prefix])
                diff.setdefault(ASN, {"to_origin": [], "summary": {"to_origin_increase": 0, "to_origin_decrease": 0, "to_origin_change": 0}})
                diff[ASN]["to_origin"].append(f'+|{prefix}|{new_str}')
                diff[ASN]["summary"]["to_origin_increase"] += 1
        
        return diff
    
    def update_db_diff(self, old_routing_file_name):
        self.db_updater.update_sp_info(self.data_dict, self.ASN_peer_dict)
        if not os.path.exists(old_routing_file_name):
            mlogger.warning(f'file {old_routing_file_name} does not exist, skip doing diff')
        elif not os.path.isfile(old_routing_file_name):
            mlogger.warning(f'{old_routing_file_name} is not a file, skip doing diff')
        else:
            self.db_updater.update_summary_info(self.compare_routing(old_routing_file_name))


def routeDiff_execute(SAVE_DIR, current_date):
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")
    d_exe = DiffExecute(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue
        
        # for testing
        #if not file.startswith('3'):
        #    continue
        d_exe.construct_routing_by_file(os.path.join(SAVE_DIR, file))
    d_exe.construct_routing_file(os.path.join(parparpar_path, f'bgpwatch/diffData/{current_date}_info'))
    d_exe.update_db_diff(os.path.join(parparpar_path, f'bgpwatch/diffData/{yesterday_date}_info'))


def routeDiff_execute_schedule():
    dir_date = datetime.now().strftime("%Y%m%d")
    SAVE_DIR = os.path.join(parparpar_path, f'bgpwatch/srcData/{dir_date}')
    current_date = datetime.now().strftime("%Y_%m_%d")
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")
    d_exe = DiffExecute(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue
        
        # for testing
        # if not file.startswith('3'):
        #    continue
        d_exe.construct_routing_by_file(os.path.join(SAVE_DIR, file))
    d_exe.filterBogon()
    d_exe.construct_routing_file(os.path.join(parparpar_path, f'bgpwatch/diffData/{current_date}_info'))
    d_exe.update_db_diff(os.path.join(parparpar_path, f'bgpwatch/diffData/{yesterday_date}_info'))
    client = pymongo.MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db['finish_tag']
    col.update_one({"_id":current_date}, {"$addToSet":{"routeDiff":{"$each":IDS}}}, upsert=True)


if __name__ == '__main__':
    routeDiff_execute_schedule()