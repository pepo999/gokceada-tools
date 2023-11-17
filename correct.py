from pymongo import MongoClient

conn_prod_db = MongoClient('mongodb://diegomary:Atreius%4062@vpp4i.nevergoback.cloud:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256', 27017)

db_prod = conn_prod_db['vpp4i']

coll_prod = db_prod['listgokcsm']

list_docs = list(coll_prod.find({}, {"_id": 0}))

for doc in coll_prod.find({"meter": {"$gte": 910000, "$lt": 920000}}, {"_id": 0}):
    coll_prod.update_one({"meter": doc["meter"]}, {"$set": {"type": "virtual_node"}})

updated_docs = list(coll_prod.find({"meter": {"$gte": 910000, "$lt": 920000}}, {"_id": 0}))
print(updated_docs)