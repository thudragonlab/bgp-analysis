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
import ipaddress
from logging.handlers import RotatingFileHandler
from routeMoniter.dbUpdater import dbUpdater
from datetime import datetime, timedelta
from rconfig import MONGO_DB, IDS, IPv4_Bogon, IPv6_Bogon

handler = RotatingFileHandler(os.path.join(parparpar_path, 'logs/routeMoniter-running.log'), encoding='UTF-8', maxBytes=5*1024*1024, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")
handler.setFormatter(formatter)
mlogger = logging.getLogger(f'routeMoniter')
mlogger.setLevel(logging.INFO)
mlogger.addHandler(handler)


class MoniterExecute():
    def __init__(self, current_date):
        self.db_updater = dbUpdater(MONGO_DB, current_date)
        self.data_dict = {}
        self.pfx_dict = {}
        self.bogon_dict = {}
    
    def _get_full_ipv6_prefix(self, pfx):
        position = pfx.find('/')
        ip = pfx[:position]
        length = pfx[position:]
        return f'{ipaddress.ip_address(ip).exploded}{length}'
    
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

    def moniter_data_by_file(self, file_dir):
        mlogger.info(f'[routeMoniter] handling file {file_dir}')
        line_counter = 0
        with open(file_dir, 'r') as f:
            for line in f:
                items = line.split('|')

                line_counter += 1
                if line_counter % 500000 == 0:
                    mlogger.info(f'[routeMoniter] file {file_dir} proccessd {line_counter} lines')
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
                for i in range(len(as_path_list) - 1):
                    ASN = as_path_list[i]
                    next_ASN = as_path_list[i + 1]

                    if ASN == as_path_list[-1]:
                        break
                    if ASN == next_ASN:
                        continue
                    self.data_dict.setdefault(ASN, {'import':{'ipv4':{},'ipv6':{}},'export':{'ipv4':{},'ipv6':{}}})
                    self.data_dict.setdefault(next_ASN, {'import':{'ipv4':{},'ipv6':{}},'export':{'ipv4':{},'ipv6':{}}})
                        
                    if '.' in pfx and ':' not in pfx:
                        self.data_dict[ASN]['import']['ipv4'].setdefault(next_ASN,set())
                        self.data_dict[ASN]['import']['ipv4'][next_ASN].add(pfx)
                        self.data_dict[next_ASN]['export']['ipv4'].setdefault(ASN,set())
                        self.data_dict[next_ASN]['export']['ipv4'][ASN].add(pfx)
                    else:
                        self.data_dict[ASN]['import']['ipv6'].setdefault(next_ASN,set())
                        self.data_dict[ASN]['import']['ipv6'][next_ASN].add(pfx)
                        self.data_dict[next_ASN]['export']['ipv6'].setdefault(ASN,set())
                        self.data_dict[next_ASN]['export']['ipv6'][ASN].add(pfx)
                
                origin_ASN = as_path_list[-1]
                if origin_ASN not in self.pfx_dict:
                    self.pfx_dict.setdefault(origin_ASN,{'prefixes':set()})
                self.pfx_dict[origin_ASN]['prefixes'].add(pfx)
        
        mlogger.info(f'[routeMoniter] file {file_dir} finished')
    
    def filterBogon(self):
        pass

    def convert_set_to_list(self):
        mlogger.info("[routeMoniter] Converting set to list")
        for ASN in self.pfx_dict:
            self.pfx_dict[ASN]['prefixes'] = list(self.pfx_dict[ASN]['prefixes'])
        
        for asn in self.data_dict:
            for keys in ['export','import']:
                for ipv in ['ipv4','ipv6']:
                    for _as in self.data_dict[asn][keys][ipv]:
                        self.data_dict[asn][keys][ipv][_as] = len(self.data_dict[asn][keys][ipv][_as])

    def update_to_db_moniter(self):
        self.convert_set_to_list()
        self.db_updater.update_to_db(self.data_dict, self.pfx_dict)


def routeMoniter_execute(SAVE_DIR, current_date):
    m_exe = MoniterExecute(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue
            
        # for testing
        #if not file.startswith('3'):
        #    continue
        m_exe.moniter_data_by_file(os.path.join(SAVE_DIR, file))
    m_exe.update_to_db_moniter()


def routeMoniter_execute_schedule():
    dir_date = datetime.now().strftime("%Y%m%d")
    SAVE_DIR = os.path.join(parparpar_path, f'bgpwatch/srcData/{dir_date}')
    current_date = datetime.now().strftime("%Y_%m_%d")
    m_exe = MoniterExecute(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue
            
        # for testing
        #if not file.startswith('3'):
        #    continue
        m_exe.moniter_data_by_file(os.path.join(SAVE_DIR, file))
    m_exe.update_to_db_moniter()
    client = pymongo.MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db['finish_tag']
    col.update_one({"_id":current_date}, {"$addToSet":{"routeMoniter":{"$each":IDS}}}, upsert=True)

if __name__ == '__main__':
    routeMoniter_execute_schedule()