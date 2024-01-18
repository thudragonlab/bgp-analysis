import pymongo
import logging
from datetime import datetime

class Updater():
    def __init__(self, MONGO_DB, current_date):
        self.MONGO_DB = MONGO_DB
        self.client = pymongo.MongoClient(host=self.MONGO_DB['host'], port=self.MONGO_DB['port'], username=self.MONGO_DB['username'], password=self.MONGO_DB['password'])
        self.db = self.client['routing_tree_info']
        
        col_name_tmp = f'prefix-count-tmp' 
        self.collection = self.db[col_name_tmp]
        self.mlogger = logging.getLogger("routeAnalyze")

    def init(self, current_date):
        self.client = pymongo.MongoClient(host=self.MONGO_DB['host'], port=self.MONGO_DB['port'], username=self.MONGO_DB['username'], password=self.MONGO_DB['password'])
        self.db = self.client['routing_tree_info']
        
        col_name_tmp = f'prefix-count-tmp' 
        self.collection = self.db[col_name_tmp]
        self.mlogger = logging.getLogger("routeAnalyze")    
    # for every ASN, insert its data
    # format of data : data[ASN] -> {"origin_ipv4_prefix":[], 
    # "origin_ipv6_prefix":[], "origin_ipv4_prefix_count":(int), 
    # "origin_ipv6_prefix_count":(int), "origin_ipv4_prefix_sum_24":(int), 
    # "origin_ipv6_prefix_sum_48":(int)}
    def update(self, data):
        self.mlogger.info("[routeAnalyze] Updating data")
        ins_list = []
        for ASN in data:
            ins_list.append(self._update_one(ASN, data[ASN]))
        if len(ins_list) > 0:
            self.collection.bulk_write(ins_list, ordered=True)

        '''ins_list = []
        for ASN in data:
            res = self._check_dict_length(ASN)
            if res == None:
                continue
            ins_list.append(res)
        if len(ins_list) > 0:
            self.collection.bulk_write(ins_list, ordered=True)
        self.mlogger.info("[routeAnalyze] Updating finished")'''
    
    def _check_dict_length(self, ASN):
        old_data = self.collection.find_one({"_id" : ASN})
        if len(old_data["origin_ipv4_prefix_count"]) < 1000:
            return None
        old_data["origin_ipv4_prefix_count"].pop(min(old_data["origin_ipv4_prefix_count"]), None)
        old_data["origin_ipv6_prefix_count"].pop(min(old_data["origin_ipv6_prefix_count"]), None)
        old_data["origin_ipv4_prefix_sum_24"].pop(min(old_data["origin_ipv4_prefix_sum_24"]), None)
        old_data["origin_ipv6_prefix_sum_48"].pop(min(old_data["origin_ipv6_prefix_sum_48"]), None)
        
        cond = {"_id":ASN}
        content = {"$set":{"origin_ipv4_prefix_count":old_data["origin_ipv4_prefix_count"],
                            "origin_ipv6_prefix_count":old_data["origin_ipv4_prefix_count"],
                            "origin_ipv4_prefix_sum_24":old_data["origin_ipv4_prefix_sum_24"],
                            "origin_ipv6_prefix_sum_48":old_data["origin_ipv4_prefix_sum_24"]}}
        return pymongo.UpdateOne(cond, content, upsert=True)
    
    def _update_one(self, ASN, data):
        data.pop("origin_ipv4_prefix")
        data.pop("origin_ipv6_prefix")
        ipv4_0, ipv4_1 = data["origin_ipv4_prefix_count"].split('|')[0], data["origin_ipv4_prefix_count"].split('|')[1]
        ipv6_0, ipv6_1 = data["origin_ipv6_prefix_count"].split('|')[0], data["origin_ipv6_prefix_count"].split('|')[1]
        sum4_0, sum4_1 = data["origin_ipv4_prefix_sum_24"].split('|')[0], data["origin_ipv4_prefix_sum_24"].split('|')[1]
        sum6_0, sum6_1 = data["origin_ipv6_prefix_sum_48"].split('|')[0], data["origin_ipv6_prefix_sum_48"].split('|')[1]
        cond = {"_id" : ASN}
        content = {"$inc":{f'origin_ipv4_prefix_count':int(ipv4_1),
                            f'origin_ipv6_prefix_count':int(ipv6_1),
                            f'origin_ipv4_prefix_sum_24':int(sum4_1),
                            f'origin_ipv6_prefix_sum_48':int(sum6_1)},
                    "$set":{f'date':ipv4_0}}
        return pymongo.UpdateOne(cond, content, upsert=True)

        
        
if __name__ == '__main__':
    pass