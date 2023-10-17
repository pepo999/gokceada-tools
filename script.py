from pymongo import MongoClient
import datetime as datetime
import matplotlib.pyplot as plt

conn_prod_db = MongoClient('mongodb://diegomary:Atreius%4062@vpp4i.nevergoback.cloud:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256', 27017)
conn_personal_db = MongoClient("mongodb+srv://pietroviglino999:rkoEZiZp6tzduEUZ@vpp4dbtest.yseft60.mongodb.net/?retryWrites=true&w=majority", 27017)

db_prod = conn_prod_db['vpp4i']
db_personal = conn_personal_db['vpp4_database_test']


def check_missing_ts(collection_name):
    prod_coll = db_prod[collection_name]
    data = list(prod_coll.find({}, {'_id': 0}))  
    # ts_start = data[0]['timestamp']
    ts_start = '2023-08-31 18:00:00'
    f_ts_start = datetime.datetime.strptime(ts_start, '%Y-%m-%d %H:%M:%S')
    ts_end = '2023-09-30 18:00:00'
    f_ts_end = datetime.datetime.strptime(ts_end, '%Y-%m-%d %H:%M:%S')
    corrected_data = []
    for doc in data:    
        if doc['timestamp'] != str(f_ts_start):
            while doc['timestamp'] != str(f_ts_start):
                corrected_doc = {"timestamp": str(f_ts_start), "name": doc['name'], "type": doc['type'], "Lat": doc['Lat'], "Long": doc['Long'], "generated": None}
                corrected_data.append(corrected_doc)
                f_ts_start += datetime.timedelta(hours=1)
        if doc['timestamp'] == str(f_ts_start):
            corrected_data.append(doc)
            f_ts_start += datetime.timedelta(hours=1)                 
        if f_ts_start > f_ts_end:
            break
    target_coll = db_personal[collection_name]
    target_data = list(target_coll.find({}, {"_id": 0}))
    for doc in corrected_data:
        if doc in target_data:
            pass
        else:
            print('adding document: ', doc)
            target_coll.insert_one(doc)
    print("done")
    return 

def impute(collection_name):
    box2 = db_personal['box2_gokc']
    data_ref = list(box2.find({}))
    data_ref = sorted(data_ref, key=lambda x: x["timestamp"])
    pers_coll = db_personal[collection_name]
    data = list(pers_coll.find({}))
    data = sorted(data, key=lambda x: x["timestamp"])
    for doc, doc_ref in zip(data, data_ref):
        # print( ' ts: ', doc['timestamp'], 'gen: ', doc['generated'],  ' 2gen: ', doc_ref['generated'], '2ts: ', doc_ref['timestamp'])
        if doc['generated'] is None and doc_ref['generated'] is not None:
            pers_coll.update_one({"_id": doc["_id"]}, {"$set": {"generated": doc_ref["generated"]}})
    print('done')

def plot_coll(collection_name):
    pers_coll = db_personal[collection_name]
    data = list(pers_coll.find({}, {"_id": 0}))
    data = sorted(data, key=lambda x: x["timestamp"])
    x = [datetime.datetime.strptime(_['timestamp'], '%Y-%m-%d %H:%M:%S') for _ in data]
    y = [float(_['generated']) for _ in data]
    plt.plot(x, y)
    plt.xlabel('Timestamp')
    plt.ylabel('Generated Values')
    plt.title(f'Plot for {collection_name}')
    plt.xticks(rotation=45)
    plt.show()

# check_missing_ts("box1_gokc")

# impute('box1_gokc')

plot_coll('box3_gokc')
