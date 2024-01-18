# function of getHash :
# 1. 
# 2. 


import logging
import os
import json
import pymongo
import hashlib
import ipaddress
from pymongo.write_concern import WriteConcern
from datetime import datetime

class DbUpdater():
    def __init__(self, MONGO_DB, current_date):
        self.MONGO_DB = MONGO_DB
        self.client = pymongo.MongoClient(host=self.MONGO_DB['host'], port=self.MONGO_DB['port'], username=self.MONGO_DB['username'], password=self.MONGO_DB['password'])
        self.db = self.client['routing_tree_info']
        t_col_name_tmp = f'tree-hash-{current_date}-tmp'
        as_col_name_tmp = f'as-info-{current_date}-tmp'
        pfx_col_name_tmp = f'prefix-tree-info-{current_date}-tmp'
        self.tree_collection = self.db[t_col_name_tmp]
        self.as_collection = self.db[as_col_name_tmp]
        self.pfx_collection = self.db[pfx_col_name_tmp]
        self.ASN_dict = {}
        self.Hash_dict = {}
        self.pfx_dict = {}
        self.mlogger = logging.getLogger(f'treeHash')
        self.first_hops_dict = {}
        self.pfx_tree_dict = {}
        
    def init(self, current_date):
        self.client = pymongo.MongoClient(host=self.MONGO_DB['host'], port=self.MONGO_DB['port'], username=self.MONGO_DB['username'], password=self.MONGO_DB['password'])
        self.db = self.client['routing_tree_info']
        t_col_name_tmp = f'tree-hash-{current_date}-tmp'
        as_col_name_tmp = f'as-info-{current_date}-tmp'
        pfx_col_name_tmp = f'prefix-tree-info-{current_date}-tmp'
        self.tree_collection = self.db[t_col_name_tmp]
        self.as_collection = self.db[as_col_name_tmp]
        self.pfx_collection = self.db[pfx_col_name_tmp]
        self.ASN_dict = {}
        self.Hash_dict = {}
        self.pfx_dict = {}
        self.mlogger = logging.getLogger(f'treeHash')
        self.first_hops_dict = {}
        self.pfx_tree_dict = {}
    
    def _getFullAdress(self, pfx):
        full_address = ''
        if ':' in pfx:
            pos = pfx.find('/')
            full_address = ipaddress.ip_address(pfx[0:pos]).exploded + pfx[pos:]
        else:
            full_address = pfx
        return full_address

    def _ip_to_binary(self, ip_address):
        # 如果是IPv4地址
        if '.' in ip_address:
            # 将IPv4地址转换为32位二进制
            binary_ip = bin(int(''.join(['{:08b}'.format(int(octet)) for octet in ip_address.split('.')]), 2))[2:]
            # 补全32位
            binary_ip = '0' * (32 - len(binary_ip)) + binary_ip
        # 如果是IPv6地址
        else:
            # 将IPv6地址转换为128位二进制
            binary_ip = bin(int(''.join(['{:04x}'.format(int(octet, 16)) for octet in ip_address.split(':')]), 16))[2:]
            # 补全128位
            binary_ip = '0' * (128 - len(binary_ip)) + binary_ip
        return binary_ip


    def _get_binary_prefix(self, prefix):
        pos = prefix.find('/')
        return self._ip_to_binary(prefix[0:pos]) + prefix[pos:]
    
    def _fastpow(self, a, b, c):
        ret, base = 1, a
        while b > 0:
            if b & 1 == 1:
                ret = ret * base % c
            base = base * base % c
            b >>= 1
        return ret
    
    def update_pfx_tree_dict(self, prefix, path):
        if prefix not in self.pfx_tree_dict:
            self.pfx_tree_dict.setdefault(prefix, {})

        origin_ASN = path[-1]
        if origin_ASN not in self.pfx_tree_dict[prefix]:
            self.pfx_tree_dict[prefix].setdefault(origin_ASN, {})
        for i in range(len(path) - 1):
            ASN, next_ASN = path[i], path[i + 1]
            if ASN == origin_ASN:
                break
            if ASN == next_ASN:
                continue
            if next_ASN not in self.pfx_tree_dict[prefix][origin_ASN]:
                self.pfx_tree_dict[prefix][origin_ASN].setdefault(next_ASN, set())
            self.pfx_tree_dict[prefix][origin_ASN][next_ASN].add(ASN)
    
    def _get_listed_dict(self, tree_dict):
        res = {}
        for key in tree_dict:
            res[key] = list(tree_dict[key])
        return res
    
    def _get_tree_hash_and_first_hops(self, tree_dict, root):
        res, pair_list = '', []
        for ASN in tree_dict:
            for next_ASN in tree_dict[ASN]:
                pair_list.append((int(next_ASN), int(ASN)))
        pair_str = str(sorted(pair_list))
        sha256_hash = hashlib.sha256()
        sha256_hash.update(pair_str.encode())
        res = sha256_hash.hexdigest()
        first_hops = set()
        if root in tree_dict:
            first_hops = tree_dict[root]
        if len(pair_list) == 0:
            return "-1", set()
        return res, first_hops
    
    def _get_tree_hash_and_first_hops_mul(self, tree_dict, roots):
        res, pair_list = '', []
        for ASN in tree_dict:
            for next_ASN in tree_dict[ASN]:
                pair_list.append((int(next_ASN), int(ASN)))
        pair_str = str(sorted(pair_list))
        sha256_hash = hashlib.sha256()
        sha256_hash.update(pair_str.encode())
        res = sha256_hash.hexdigest()
        first_hops = set()
        for root in roots:
            if root in tree_dict:
                first_hops = first_hops | tree_dict[root]
        if len(pair_list) == 0:
            return "-1", set()
        return res, first_hops

    def _get_full_tree_dict(self, dict):
        res = {}
        for root in dict:
            for key in dict[root]:
                if key not in res:
                    res.setdefault(key, set())
                for nASN in dict[root][key]:
                    res[key].add(nASN)
        return res

    def update_to_db(self):
        self.mlogger.info("[treeHash] Updating route trees to mongodb")
        t_ins_list, p_ins_list = [], []
        for prefix in self.pfx_tree_dict:
            L, roots = len(self.pfx_tree_dict[prefix]), []
            if L == 1:
                pfx = self._getFullAdress(prefix)
                for root in self.pfx_tree_dict[prefix]:
                    roots.append(root)
                R = roots[0]
                res, first_hops = self._get_tree_hash_and_first_hops(self.pfx_tree_dict[prefix][R], R)
                p_ins_list.append(pymongo.UpdateOne({"_id":pfx}, {"$set": {"binary":self._get_binary_prefix(pfx), "Hash":res, "AS":roots}}, upsert=True))
                if res == "-1":
                    continue
                if root not in self.ASN_dict:
                    self.ASN_dict.setdefault(root, {"_id":root, "Hashes":{}, "all_Hashes_info":set()})
                if res not in self.Hash_dict:
                    self.Hash_dict.setdefault(res, True)
                    t_ins_list.append(pymongo.UpdateOne({"_id":res}, {"$set":{"edges":self._get_listed_dict(self.pfx_tree_dict[prefix][R]),
                                                                            "ASroots":[R], "Allroots":[R], "first_hops":sorted(first_hops)}},
                                                                            upsert=True))
                    if res not in self.ASN_dict[root]["Hashes"]:
                        self.ASN_dict[root]["Hashes"].setdefault(res, set())
                        md5_hash = hashlib.md5()
                        md5_hash.update(str(sorted(first_hops)).encode())
                        first_hop_hash = md5_hash.hexdigest()
                        self.ASN_dict[root]["all_Hashes_info"].add(json.dumps({'val' : res, 'first_hops' : sorted(first_hops), 'first_hop_hash' : first_hop_hash}))
                else:
                    if res not in self.ASN_dict[root]["Hashes"]:
                        self.ASN_dict[root]["Hashes"].setdefault(res, set())
                        md5_hash = hashlib.md5()
                        md5_hash.update(str(sorted(first_hops)).encode())
                        first_hop_hash = md5_hash.hexdigest()
                        self.ASN_dict[root]["all_Hashes_info"].add(json.dumps({'val' : res, 'first_hops' : sorted(first_hops), 'first_hop_hash' : first_hop_hash}))
                self.ASN_dict[root]["Hashes"][res].add(pfx)
            else:
                pfx = self._getFullAdress(prefix)
                for root in self.pfx_tree_dict[prefix]:
                    roots.append(root)
                    res, first_hops = self._get_tree_hash_and_first_hops(self.pfx_tree_dict[prefix][root], root)
                    if res == "-1":
                        continue
                    if root not in self.ASN_dict:
                        self.ASN_dict.setdefault(root, {"_id":root, "Hashes":{}, "all_Hashes_info":set()})
                    if res not in self.Hash_dict:
                        self.Hash_dict.setdefault(res, True)
                        t_ins_list.append(pymongo.UpdateOne({"_id":res}, {"$set":{"edges":self._get_listed_dict(self.pfx_tree_dict[prefix][root]),
                                                                          "ASroots":[root], "Allroots":[root], "first_hops":sorted(first_hops)}},
                                                                          upsert=True))
                        if res not in self.ASN_dict[root]["Hashes"]:
                            self.ASN_dict[root]["Hashes"].setdefault(res, set())
                            md5_hash = hashlib.md5()
                            md5_hash.update(str(sorted(first_hops)).encode())
                            first_hop_hash = md5_hash.hexdigest()
                            self.ASN_dict[root]["all_Hashes_info"].add(json.dumps({'val' : res, 'first_hops' : sorted(first_hops), 'first_hop_hash' : first_hop_hash}))
                    else:
                        if res not in self.ASN_dict[root]["Hashes"]:
                            self.ASN_dict[root]["Hashes"].setdefault(res, set())
                            md5_hash = hashlib.md5()
                            md5_hash.update(str(sorted(first_hops)).encode())
                            first_hop_hash = md5_hash.hexdigest()
                            self.ASN_dict[root]["all_Hashes_info"].add(json.dumps({'val' : res, 'first_hops' : sorted(first_hops), 'first_hop_hash' : first_hop_hash}))
                    self.ASN_dict[root]["Hashes"][res].add(pfx)

                z_tree_dict = self._get_full_tree_dict(self.pfx_tree_dict[prefix])
                res, first_hops = self._get_tree_hash_and_first_hops_mul(z_tree_dict, roots)
                p_ins_list.append(pymongo.UpdateOne({"_id":pfx}, {"$set": {"binary":self._get_binary_prefix(pfx), "Hash":res, "AS":roots}}, upsert=True))
                if res == "-1":
                    continue
                if res not in self.Hash_dict:
                    self.Hash_dict.setdefault(res, True)
                    t_ins_list.append(pymongo.UpdateOne({"_id":res}, {"$set":{"edges":self._get_listed_dict(z_tree_dict),
                                                                          "ASroots":roots, "Allroots":roots, "first_hops":sorted(first_hops)}},
                                                                          upsert=True))
                else:
                    t_ins_list.append(pymongo.UpdateOne({"_id":res}, {"$set":{"Allroots":roots}}, upsert=True))
                
            if len(t_ins_list) >= 30000:
                self.tree_collection.bulk_write(t_ins_list, ordered=True)
                self.mlogger.info(f'[treeHash] updated {len(t_ins_list)} hash infos')
                t_ins_list = []
            
            if len(p_ins_list) >= 30000:
                self.pfx_collection.bulk_write(p_ins_list, ordered=True)
                self.mlogger.info(f'[treeHash] updated {len(p_ins_list)} prefix infos')
                p_ins_list = []

        if len(t_ins_list) > 0:
            self.tree_collection.bulk_write(t_ins_list, ordered=True)
            self.mlogger.info(f'[treeHash] updated {len(t_ins_list)} hash infos')
        
        if len(p_ins_list) > 0:
            self.pfx_collection.bulk_write(p_ins_list, ordered=True)
            self.mlogger.info(f'[treeHash] updated {len(p_ins_list)} prefix infos')

        a_ins_list = []
        for ASN in self.ASN_dict:
            Hashes, cond = {}, {"_id":ASN}
            for Hash in self.ASN_dict[ASN]["Hashes"]:
                Hashes[f'Hashes.{Hash}'] = {"$each":list(self.ASN_dict[ASN]["Hashes"][Hash])}
            Hashes['all_Hashes_info'] = {"$each":list(self.ASN_dict[ASN]['all_Hashes_info'])}
            a_ins_list.append(pymongo.UpdateOne(cond, {"$addToSet":Hashes}, upsert=True))
        self.as_collection.bulk_write(a_ins_list, ordered=True)
        self.mlogger.info("Updating finished")

if __name__ == '__main__':
    pass