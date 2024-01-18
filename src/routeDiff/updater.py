import pymongo
import logging
from pymongo.write_concern import WriteConcern
from datetime import datetime

class Updater():
    def __init__(self, MONGO_DB, current_date):
        self.MONGO_DB = MONGO_DB
        self.client = pymongo.MongoClient(host=self.MONGO_DB['host'], port=self.MONGO_DB['port'], username=self.MONGO_DB['username'], password=self.MONGO_DB['password'])
        self.db = self.client['routing_tree_info']
        col_name_tmp = f'diff-info-{current_date}-tmp'
        col_peer_tmp = f'peer-info-specific-{current_date}-tmp'
        col_sum_name_tmp = f'diff-summary-{current_date}-tmp'
        self.collection = self.db[col_name_tmp]
        self.collection_sum = self.db[col_sum_name_tmp]
        self.collection_peer = self.db[col_peer_tmp]
        
        self.mlogger = logging.getLogger(f'routeDiff')

    def update_sp_info(self, data_dict, ASN_peer_dict):
        self.mlogger.info("Updating data to MongoDB")
        pfx_dict = {}
        for ASN in data_dict:
            if len(data_dict[ASN]["to_origin"]) == 0:
                continue
            for prefix in data_dict[ASN]["to_origin"]:
                if prefix not in pfx_dict:
                    pfx_dict.setdefault(prefix, {})
                if ASN not in pfx_dict[prefix]:
                    pfx_dict[prefix].setdefault(ASN, [])
                #print(type(ASN))
                if '-1' in data_dict[ASN]["to_origin"][prefix]:
                    data_dict[ASN]["to_origin"][prefix].discard('-1')
                pfx_dict[prefix][ASN] = list(data_dict[ASN]["to_origin"][prefix])
        
        ins_list = []
        for prefix in pfx_dict:
            ins_dict = {"_id":prefix, "to_origin":{}}
            for ASN in pfx_dict[prefix]:
                ins_dict["to_origin"][ASN] = pfx_dict[prefix][ASN]
            ins_list.append(ins_dict)
            if len(ins_list) >= 40000:
                self.collection.with_options(write_concern=WriteConcern(w=0)).insert_many(ins_list)
                # print(f'update_sp_info_diff : updated {len(ins_list)} infos')
                self.mlogger.info(f'update_sp_info_diff : updated {len(ins_list)} infos')
                ins_list = []

        if len(ins_list) > 0:
            self.collection.with_options(write_concern=WriteConcern(w=0)).insert_many(ins_list)
        
        self.mlogger.info(f'update_sp_info_diff : Updating ASN peer info')
        for ASN in ASN_peer_dict:
            cond = {"_id":ASN}
            List = list(ASN_peer_dict[ASN])
            content = {"$addToSet":{"peers":{"$each":List}}}
            self.collection_peer.update_one(cond, content, upsert=True)
    
    def update_summary_info(self, diff_dict):
        for ASN in diff_dict:
            cond = {"_id" : ASN}
            content = {"$addToSet":{"to_origin":{"$each":diff_dict[ASN]["to_origin"]}}, "$inc":{"summary.to_origin_increase":diff_dict[ASN]["summary"]["to_origin_increase"],
                        "summary.to_origin_decrease":diff_dict[ASN]["summary"]["to_origin_decrease"],
                        "summary.to_origin_change":diff_dict[ASN]["summary"]["to_origin_change"]}}
            self.collection_sum.update_one(cond, content, upsert=True)
        #self.collection_sum.rename(self.col_sum_name)


if __name__ == '__main__':
    pass
