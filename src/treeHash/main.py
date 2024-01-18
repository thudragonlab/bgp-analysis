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

import logging
import json
import pymongo
import ipaddress
from logging.handlers import RotatingFileHandler
from treeHash.dbUpdater import DbUpdater
from datetime import datetime, timedelta
from rconfig import MONGO_DB, IDS

handler = RotatingFileHandler(os.path.join(parparpar_path, 'logs/treeHash-running.log'), encoding='UTF-8', maxBytes=5*1024*1024, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")
handler.setFormatter(formatter)
mlogger = logging.getLogger(f'treeHash')
mlogger.setLevel(logging.INFO)
mlogger.addHandler(handler)

class HashExecute():
    def __init__(self, current_date):
        self.db_updater = DbUpdater(MONGO_DB, current_date)
    
    def hash_init(self, current_date):
        self.db_updater.init(current_date)
    
    def _get_full_ipv6_prefix(self, pfx):
        position = pfx.find('/')
        ip = pfx[:position]
        length = pfx[position:]
        return f'{ipaddress.ip_address(ip).exploded}{length}'
    
    def get_hash_by_file(self, file_dir):
        mlogger.info(f'[treeHash] handling file {file_dir}')
        line_counter = 0
        with open(file_dir, 'r') as f:
            for line in f:
                items = line.split('|')

                line_counter += 1
                if line_counter % 500000 == 0:
                    mlogger.info(f'[treeHash] file {file_dir} proccessd {line_counter} lines')
                # not the correct format
                if not len(items) == 15:
                    continue
                
                _, ts, b, nh, vp, pfx, as_path, origin, nh, _1, _2, community, agg, _3, _4 = items

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
                    
                self.db_updater.update_pfx_tree_dict(pfx, as_path.split(' '))

        mlogger.info(f'[treeHash] file {file_dir} finished')
    
    def update_db_getHash(self):
        self.db_updater.update_to_db()


def treeHash_execute(SAVE_DIR, current_date):
    h_exe = HashExecute(current_date)
    h_exe.hash_init(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue

        # for testing
        #if not file.startswith('3') and not file.startswith('4'):
        #    continue
        h_exe.get_hash_by_file(os.path.join(SAVE_DIR, file))
    h_exe.update_db_getHash()


def treeHash_execute_schedule():
    dir_date = datetime.now().strftime("%Y%m%d")
    SAVE_DIR = os.path.join(parparpar_path, f'bgpwatch/srcData/{dir_date}')
    current_date = datetime.now().strftime("%Y_%m_%d")
    h_exe = HashExecute(current_date)
    h_exe.hash_init(current_date)
    for file in os.listdir(SAVE_DIR):
        if not file.endswith('.txt'):
            continue

        # for testing
        #if not file.startswith('3') and not file.startswith('4'):
        #    continue
        h_exe.get_hash_by_file(os.path.join(SAVE_DIR, file))
    h_exe.update_db_getHash()

    client = pymongo.MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db['finish_tag']
    col.update_one({"_id":current_date}, {"$addToSet":{"treeHash":{"$each":IDS}}}, upsert=True)


if __name__ == '__main__':
    '''current_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    SAVE_DIR = f'/home/taoyx/donly-bgp-routing/bgpwatch/srcData/{current_date}'
    treeHash_execute(SAVE_DIR, current_date)'''
    treeHash_execute_schedule()
