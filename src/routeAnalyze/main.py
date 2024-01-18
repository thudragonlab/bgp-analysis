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
from datetime import datetime
from logging.handlers import RotatingFileHandler
from routeAnalyze.updater import Updater
from rconfig import MONGO_DB, IDS, IPv4_Bogon, IPv6_Bogon

handler = RotatingFileHandler(os.path.join(parparpar_path, 'logs/routeAnalyze-running.log'), encoding='UTF-8', maxBytes=5*1024*1024, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")
handler.setFormatter(formatter)
mlogger = logging.getLogger(f'routeAnalyze')
mlogger.setLevel(logging.INFO)
mlogger.addHandler(handler)

class AnalyzeExecute():
    def __init__(self, current_date):
        self.current_date = current_date
        self.db_updater = Updater(MONGO_DB, current_date)
        self.routing_data = {}

    def analyzer_init(self, current_date):
        self.db_updater.init(current_date)
        self.routing_data = {}
    
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

    def analyze_data_by_file(self, file_dir):
        mlogger.info(f'[routeAnalyze] handling file {file_dir}')
        line_counter = 0
        with open(file_dir, 'r') as f:
            for line in f:
                items = line.split('|')

                line_counter += 1
                if line_counter % 500000 == 0:
                    mlogger.info(f'[routeAnalyze] file {file_dir} proccessd {line_counter} lines')
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

                origin_ASN = as_path.split(' ')[-1]
                if origin_ASN not in self.routing_data:
                    self.routing_data[origin_ASN] = {"origin_ipv4_prefix": set(), "origin_ipv6_prefix": set()}

                if '.' in pfx and ':' not in pfx:
                    self.routing_data[origin_ASN]["origin_ipv4_prefix"].add(pfx)
                else:
                    self.routing_data[origin_ASN]["origin_ipv6_prefix"].add(pfx)
        
        mlogger.info(f'[routeAnalyze] file {file_dir} finished')

    def analyze(self, update_time):
        mlogger.info("[routeAnalyze] Analyzing data")
        for ASN in self.routing_data:
            # filter bogon
            '''for pfx in list(self.routing_data[ASN]["origin_ipv4_prefix"]):
                flag, bogon = self._isBogon(pfx)
                if flag == True:
                    self.routing_data[ASN]["origin_ipv4_prefix"].remove(pfx)
            for pfx in list(self.routing_data[ASN]["origin_ipv6_prefix"]):
                flag, bogon = self._isBogon(pfx)
                if flag == True:
                    self.routing_data[ASN]["origin_ipv6_prefix"].remove(pfx)'''

            # sort
            self.routing_data[ASN]["origin_ipv4_prefix"] = sorted(list(self.routing_data[ASN]["origin_ipv4_prefix"]), key=lambda x: tuple(map(int, x.split('/')[0].split('.'))))
            self.routing_data[ASN]["origin_ipv6_prefix"] = sorted(list(self.routing_data[ASN]["origin_ipv6_prefix"]), key=lambda x: tuple(x.split('/')[0].split(':')))
        
            # count prefix num and sum
            self.routing_data[ASN]["origin_ipv4_prefix_count"] = f'{update_time}|{len(self.routing_data[ASN]["origin_ipv4_prefix"])}'
            self.routing_data[ASN]["origin_ipv6_prefix_count"] = f'{update_time}|{len(self.routing_data[ASN]["origin_ipv6_prefix"])}'
            
            ipv4_sum = 0
            for prefix in self.routing_data[ASN]['origin_ipv4_prefix']:
                T = int(prefix.split('/')[1])
                if T <= 24:
                    ipv4_sum += 2 ** (24 - T)
            self.routing_data[ASN]["origin_ipv4_prefix_sum_24"] = f'{update_time}|{ipv4_sum}'
            
            ipv6_sum = 0
            for prefix in self.routing_data[ASN]['origin_ipv6_prefix']:
                T = int(prefix.split('/')[1])
                if T <= 48:
                    ipv6_sum += 2 ** (48 - T)
            self.routing_data[ASN]["origin_ipv6_prefix_sum_48"] = f'{update_time}|{ipv6_sum}'

    def update_db_analyze(self):
        self.analyze(self.current_date)
        self.db_updater.update(self.routing_data)


def routeAnalyze_execute(SAVE_DIR, current_date):
    a_exe = AnalyzeExecute(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue

        # for testing
        # if not file.startswith('3'):
        #   continue
        a_exe.analyze_data_by_file(os.path.join(SAVE_DIR, file))
    a_exe.update_db_analyze()


def routeAnalyze_execute_schedule():
    dir_date = datetime.now().strftime("%Y%m%d")
    SAVE_DIR = os.path.join(parparpar_path, f'bgpwatch/srcData/{dir_date}')
    current_date = datetime.now().strftime("%Y_%m_%d")
    a_exe = AnalyzeExecute(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue

        # for testing
        # if not file.startswith('3'):
        #    continue
        a_exe.analyze_data_by_file(os.path.join(SAVE_DIR, file))
    a_exe.update_db_analyze()

    client = pymongo.MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db['finish_tag']
    col.update_one({"_id":current_date}, {"$addToSet":{"routeAnalyze":{"$each":IDS}}}, upsert=True)
    

if __name__ == '__main__':
    routeAnalyze_execute_schedule()