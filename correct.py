from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

connection_string=os.getenv('connection_string')
conn_prod_db = MongoClient(connection_string, 27017)

db_prod = conn_prod_db['vpp4i']

coll_prod = db_prod['listgokcsm']

list_docs = list(coll_prod.find({}, {"_id": 0}))

for doc in coll_prod.find({"meter": {"$gte": 910000, "$lt": 920000}}, {"_id": 0}):
    coll_prod.update_one({"meter": doc["meter"]}, {"$set": {"type": "virtual_node"}})

updated_docs = list(coll_prod.find({"meter": {"$gte": 910000, "$lt": 920000}}, {"_id": 0}))
print(updated_docs)