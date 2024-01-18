import logging
import pymongo
import json
from datetime import datetime

class dbUpdater():
    def __init__(self, MONGO_DB, current_date):
        self.MONGO_DB = MONGO_DB
        self.client = pymongo.MongoClient(host=self.MONGO_DB['host'], port=self.MONGO_DB['port'], username=self.MONGO_DB['username'], password=self.MONGO_DB['password'])
        self.db = self.client['routing_tree_info']
        self.as_r_name_tmp = f'as-relationship-{current_date}-tmp'
        self.pfx_name_tmp = f'pfx-name-{current_date}-tmp'
        self.col_as_r = self.db[self.as_r_name_tmp]
        self.col_pfx = self.db[self.pfx_name_tmp]
        self.mlogger = logging.getLogger(f'routeMoniter')

    # data_dict format : data_dict[ASN]['import' / 'export']['ipv6' / 'ipv4'][ASN] -> (int)
    # pfx_dict format : pfx_dict[ASN]['prefixes'] -> [list of prefixes]
    '''def update(self, data_dict, pfx_dict):
        as_r_list = []
        for ASN in data_dict:
            cur = {"_id" : ASN}
            cur.update(data_dict[ASN])
            as_r_list.append(cur)
            if len(as_r_list) > 10000:
                self.col_as_r.insert_many(as_r_list)
                logging.debug(f'[MONGO UPDATE] Insert 10000 data in {self.col_as_r.name}')
                as_r_list = []
        
        if len(as_r_list) > 0:
            self.col_as_r.insert_many(as_r_list)
            as_r_list = []
        
        pfx_list = []
        for ASN in pfx_dict:
            pfx_dict[ASN]['_id'] = ASN
            pfx_list.append(pfx_dict[ASN])
            if len(pfx_list) > 10000:
                self.col_pfx.insert_many(pfx_list)
                logging.debug(f'[MONGO UPDATE] Insert 10000 data in {self.col_pfx.name}')
                pfx_list = []
        
        if len(pfx_list) > 0:
            self.col_as_r.insert_many(pfx_list)
            pfx_list = []
        
        self.col_as_r.rename(self.as_r_name)
        self.col_pfx.rename(self.pfx_name)'''
    
    def update_to_db(self, data_dict, pfx_dict):
        self.mlogger.info('[routMoniter] Updating AS-relationship')
        ins_list = []
        for ASN in data_dict:
            cond = {"_id":ASN}
            content = {"$inc":{}}
            for iASN in data_dict[ASN]['import']['ipv4']:
                content['$inc'][f'import.ipv4.{iASN}'] = data_dict[ASN]['import']['ipv4'][iASN]
            for iASN in data_dict[ASN]['import']['ipv6']:
                content['$inc'][f'import.ipv6.{iASN}'] = data_dict[ASN]['import']['ipv6'][iASN]
            for iASN in data_dict[ASN]['export']['ipv4']:
                content['$inc'][f'export.ipv4.{iASN}'] = data_dict[ASN]['export']['ipv4'][iASN]
            for iASN in data_dict[ASN]['export']['ipv6']:
                content['$inc'][f'export.ipv6.{iASN}'] = data_dict[ASN]['export']['ipv6'][iASN]
            
            ins_list.append(pymongo.UpdateOne(cond, content, upsert=True))

        self.col_as_r.bulk_write(ins_list, ordered=True)

        ins_list = []
        self.mlogger.info('[routeMoniter] Updating AS-prefixes')
        for ASN in pfx_dict:
            cond = {"_id":ASN}
            content = {"$addToSet":{"prefixes":{"$each":pfx_dict[ASN]['prefixes']}}}
            ins_list.append(pymongo.UpdateOne(cond, content, upsert=True))

        self.col_pfx.bulk_write(ins_list, ordered=True)
        #self.col_as_r.rename(self.as_r_name)
        #self.col_pfx.rename(self.pfx_name)

if __name__ == '__main__':
    pass