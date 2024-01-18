import os
abs_path = os.path.abspath(__file__)
par_path = os.path.split(abs_path)[0]
parpar_path = os.path.split(par_path)[0]
parparpar_path = os.path.split(parpar_path)[0]
par_path += '/'
parpar_path += '/'
parparpar_path += '/'
import sys
sys.path.append(parparpar_path)

import IPy
import json
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, UpdateOne
from logging.handlers import RotatingFileHandler
from configs.rconfig import MONGO_DB, IPv4_Bogon, IPv6_Bogon

handler = RotatingFileHandler(os.path.join(parparpar_path, 'logs/cluster-running.log'), encoding='UTF-8', maxBytes=5*1024*1024, backupCount=10)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s")
handler.setFormatter(formatter)
mlogger = logging.getLogger(f'cluster')
mlogger.setLevel(logging.INFO)
mlogger.addHandler(handler)

def isBogon(prefix):
    if '.' in prefix and ':' not in prefix:
        for bogon in IPv4_Bogon:
            if prefix in IPy.IP(bogon):
                return True, bogon
        return False, None
    else: # ipv6 bogon check
        for bogon in IPv6_Bogon:
            if prefix in IPy.IP(bogon):
                return True, bogon
        return False, None

def node_cluster(current_date):
    mlogger.info('Clustering')
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db[f'as-info-{current_date}-tmp']
    # col = db[f'as-info-{current_date}']
    ins_col = db[f'cluster-info-{current_date}-tmp']
    result = col.find({})
    ASN_counter = 0
    for res in result:
        h_dict, t_dict = {}, {}
        ASN = res['_id']
        for thash in res['Hashes']:
            t_dict[thash] = res['Hashes'][thash]
        for s_str in res['all_Hashes_info']:
            s_dict = json.loads(s_str)
            thash, first_hops, fhash = s_dict['val'], s_dict['first_hops'], s_dict['first_hop_hash']
            if fhash not in h_dict:
                h_dict.setdefault(fhash, {'value':first_hops, 'Size': 0, 'Hashes':[]})
            #print(s_dict)
            #print(thash)
            #print(fhash)
            #print(ASN)
            h_dict[fhash]['Hashes'].append([thash, t_dict[str(thash)]])
            h_dict[fhash]['Size'] += len(t_dict[str(thash)])
        h_dict['_id'] = ASN
        ins_col.insert_one(h_dict)
        ASN_counter += 1
        if ASN_counter % 10000 == 0:
            mlogger.info(f'Clustered {ASN_counter} ASNs')


def peer_count(current_date):
    mlogger.info('Counting peer num')
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db[f'peer-info-specific-{current_date}-tmp']
    col_new = db[f'peer-info']
    result = col.find({})
    ASN_counter = 0
    for res in result:
        L = res['peers']
        cond = {"_id":res['_id']}
        content = {"$inc":{f'peer_num.{current_date}':len(L)}}
        col_new.update_one(cond, content, upsert=True)
        ASN_counter += 1
        if ASN_counter % 10000 == 0:
            mlogger.info(f'Counted {ASN_counter} ASNs')
    for res in result:
        cond = {"_id":res['_id']}
        content = col_new.find_one(cond)
        if len(content['peer_num']) < 1000:
            continue
        content['peer_num'].pop(min(content['peer_num']), None)
        col_new.update_one(cond, {"$set":{"peer_num":content['peer_num']}})


def peer_diff(current_date):
    yesterday_date = (datetime.now() - timedelta(days=1)).strftime("%Y_%m_%d")
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col_today = db[f'peer-info-specific-{current_date}-tmp']
    col_yesterday = db[f'peer-info-specific-{yesterday_date}']
    res_today = col_today.find({})
    res_yesterday = col_yesterday.find({})
    yesterday_dict = {}
    for res in res_yesterday:
        yesterday_dict.setdefault(res['_id'], res['peers'])

    req_list = []
    for res in res_today:
        today_list = res['peers']
        if res['_id'] not in yesterday_dict:
            diff_list = []
            for ASN in today_list:
                diff_list.append(f'+|{ASN}')
            req_list.append(UpdateOne({"_id":res['_id']}, {"$set":{"diff":diff_list}}, upsert=True))
        else:
            diff_list = []
            yesterday_list = yesterday_dict[res['_id']]
            for ASN in today_list:
                if ASN not in yesterday_list:
                    diff_list.append(f'+|{ASN}')
            for ASN in yesterday_list:
                if ASN not in today_list:
                    diff_list.append(f'-|{ASN}')
            req_list.append(UpdateOne({"_id":res['_id']}, {"$set":{"diff":diff_list}}, upsert=True))
    
    if len(req_list) > 0:
        col_today.bulk_write(req_list, ordered=True)


def prefix_count(current_date):
    mlogger.info('Copying prefix count')
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col_old = db['prefix-count-tmp']
    col_new = db['prefix-count']
    results = col_old.find({})
    up_ins_list = []
    for res in results:
        ASN = res['_id']
        old_data = col_new.find_one({"_id" : ASN})
        if old_data == None:
            date = res['date']
            content = {"$set":{f'origin_ipv4_prefix_count.{date}':res['origin_ipv4_prefix_count'],
                               f'origin_ipv6_prefix_count.{date}':res['origin_ipv6_prefix_count'],
                               f'origin_ipv4_prefix_sum_24.{date}':res['origin_ipv4_prefix_sum_24'],
                               f'origin_ipv6_prefix_sum_48.{date}':res['origin_ipv6_prefix_sum_48']
                               }}
            up_ins_list.append(UpdateOne({"_id":ASN}, content, upsert=True))
            continue
        if len(old_data["origin_ipv4_prefix_count"]) >= 1000:
            old_data["origin_ipv4_prefix_count"].pop(min(old_data["origin_ipv4_prefix_count"]), None)
            old_data["origin_ipv6_prefix_count"].pop(min(old_data["origin_ipv6_prefix_count"]), None)
            old_data["origin_ipv4_prefix_sum_24"].pop(min(old_data["origin_ipv4_prefix_sum_24"]), None)
            old_data["origin_ipv6_prefix_sum_48"].pop(min(old_data["origin_ipv6_prefix_sum_48"]), None)
        old_data["origin_ipv4_prefix_count"][res['date']] = res['origin_ipv4_prefix_count']
        old_data["origin_ipv6_prefix_count"][res['date']] = res['origin_ipv6_prefix_count']
        old_data["origin_ipv4_prefix_sum_24"][res['date']] = res['origin_ipv4_prefix_sum_24']
        old_data["origin_ipv6_prefix_sum_48"][res['date']] = res['origin_ipv6_prefix_sum_48']
        up_ins_list.append(UpdateOne({"_id":ASN}, {"$set":old_data}, upsert=True))
    if len(up_ins_list) > 0:
        col_new.bulk_write(up_ins_list, ordered=True)
    col_old.drop()


def filterBogon(current_date):
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db[f'pfx-name-{current_date}']
    colc = db[f'prefix-count']
    results = col.find({})
    up_ins_list = []
    pc_ins_list = []
    for res in results:
        pL = res['prefixes']
        res['bogon_prefixes'], res['prefixes'] = [], []
        for pfx in pL:
            flag, bogon = isBogon(pfx)
            if flag == True:
                res['bogon_prefixes'].append(f'{pfx} in {bogon}')
            else:
                res['prefixes'].append(pfx)
        up_ins_list.append(UpdateOne({"_id":res['_id']}, {"$set":res}, upsert=True))
        AS, L = res['_id'], len(res['bogon_prefixes'])
        if L > 0:
            set_dict = {'_id':AS, f"origin_ipv4_prefix_count.{current_date}":0, f"origin_ipv6_prefix_count.{current_date}":0, f"origin_ipv4_prefix_sum_24.{current_date}":0, f"origin_ipv6_prefix_sum_48.{current_date}":0}
            for pfx in res['prefixes']:
                if '.' in pfx and ':' not in pfx:
                    set_dict[f"origin_ipv4_prefix_count.{current_date}"] += 1
                    T = int(pfx.split('/')[1])
                    if T <= 24:
                        set_dict[f"origin_ipv4_prefix_sum_24.{current_date}"] += 2 ** (24 - T)
                else:
                    set_dict[f"origin_ipv6_prefix_count.{current_date}"] += 1
                    T = int(pfx.split('/')[1])
                    if T <= 48:
                        set_dict[f"origin_ipv6_prefix_sum_48.{current_date}"] += 2 ** (48 - T)
            pc_ins_list.append(UpdateOne({'_id':AS}, {'$set':set_dict}, upsert=True))
            print(AS, L)

    if len(pc_ins_list) > 0:
        colc.bulk_write(pc_ins_list, ordered=True)
    if len(up_ins_list) > 0:     
        col.bulk_write(up_ins_list, ordered=True)

def rename_collections(current_date):
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db[f'as-info-{current_date}-tmp']
    col.rename(f'as-info-{current_date}')
    col = db[f'as-relationship-{current_date}-tmp']
    col.rename(f'as-relationship-{current_date}')
    col = db[f'cluster-info-{current_date}-tmp']
    col.rename(f'cluster-info-{current_date}')
    col = db[f'diff-info-{current_date}-tmp']
    col.rename(f'diff-info-{current_date}')
    col = db[f'diff-summary-{current_date}-tmp']
    col.rename(f'diff-summary-{current_date}')
    col = db[f'peer-info-specific-{current_date}-tmp']
    col.rename(f'peer-info-specific-{current_date}')
    col = db[f'pfx-name-{current_date}-tmp']
    col.rename(f'pfx-name-{current_date}')
    col = db[f'prefix-tree-info-{current_date}-tmp']
    col.rename(f'prefix-tree-info-{current_date}')
    col = db[f'tree-hash-{current_date}-tmp']
    col.rename(f'tree-hash-{current_date}')

def main_execute():
    current_date = datetime.now().strftime("%Y_%m_%d")
    client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db[f'finish_tag']
    result = col.find_one({'_id':current_date})
    time_counter = 0
    while result == None:
        mlogger.info('Waiting for all partition finished (None)')
        time.sleep(300)
        time_counter += 300
        if time_counter >= 3900:
            mlogger.error(f'Waited for too long, cluster aborted')
            return
        result = col.find_one({'_id':current_date})

    time_counter = 0
    flag = (len(result['routeDiff']) == 16) and (len(result['routeMoniter']) == 16) and (len(result['routeAnalyze']) == 16) and (len(result['treeHash']) == 16)
    while flag == False:
        mlogger.info('Waiting for all partition finished ( < 16)')
        time.sleep(300)
        time_counter += 300
        if time_counter >= 3900:
            mlogger.error(f'Waited for too long, cluster aborted')
            return
        result = col.find_one({'_id':current_date})
        flag = (len(result['routeDiff']) == 16) and (len(result['routeMoniter']) == 16) and (len(result['routeAnalyze']) == 16) and (len(result['treeHash']) == 16)
    node_cluster(current_date)
    peer_count(current_date)
    peer_diff(current_date)
    rename_collections(current_date)
    prefix_count(current_date)
    filterBogon(current_date)
    col = db[f'prefix-tree-info-{current_date}']
    col.create_index('binary')
    mlogger.info('execute finished')


if __name__ == '__main__':
    # node_cluster(f'2023_08_11')
    # current_date = datetime.now().strftime("%Y_%m_%d")
    current_date = f'2023_12_27'
    # main_execute(current_date)
    #node_cluster(current_date)
    #peer_count(current_date)
    #rename_collections(current_date)
    #peer_diff(current_date)
    '''client = MongoClient(host=MONGO_DB['host'], port=MONGO_DB['port'], username=MONGO_DB['username'], password=MONGO_DB['password'])
    db = client['routing_tree_info']
    col = db['prefix-tree-info-2023_08_18']
    L = col.list_indexes()
    for i in L:
        print(i)
    col.create_index('binary')
    L = col.list_indexes()
    for i in L:
        print(i)'''
    main_execute()
    # filterBogon(current_date)
    # print(isBogon('100.100.30.224/30'))