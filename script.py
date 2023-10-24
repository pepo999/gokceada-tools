from pymongo import MongoClient
import datetime as dt
from datetime import datetime
import matplotlib.pyplot as plt
from waitress import serve
import requests
from flask import Flask, jsonify
import json
from itertools import groupby

conn_prod_db = MongoClient('mongodb://diegomary:Atreius%4062@vpp4i.nevergoback.cloud:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256', 27017)
conn_personal_db = MongoClient("mongodb+srv://pietroviglino999:rkoEZiZp6tzduEUZ@vpp4dbtest.yseft60.mongodb.net/?retryWrites=true&w=majority", 27017)

db_prod = conn_prod_db['vpp4i']
db_personal = conn_personal_db['vpp4_database_test']

app = Flask(__name__, static_url_path='', static_folder='production')

@app.route('/fillmissing/<collection_name>')
def fill_missing_ts(collection_name):
    prod_coll = db_prod[collection_name]
    data = list(prod_coll.find({}, {'_id': 0}))  
    # ts_start = data[0]['timestamp']
    ts_start = '2023-08-31 18:00:00'
    f_ts_start = dt.datetime.strptime(ts_start, '%Y-%m-%d %H:%M:%S')
    ts_end = '2023-09-30 18:00:00'
    f_ts_end = dt.datetime.strptime(ts_end, '%Y-%m-%d %H:%M:%S')
    corrected_data = []
    for doc in data:    
        if doc['timestamp'] != str(f_ts_start):
            while doc['timestamp'] != str(f_ts_start):
                corrected_doc = {"timestamp": str(f_ts_start), "name": doc['name'], "type": doc['type'], "Lat": doc['Lat'], "Long": doc['Long'], "generated": None}
                corrected_data.append(corrected_doc)
                f_ts_start += dt.timedelta(hours=1)
        if doc['timestamp'] == str(f_ts_start):
            corrected_data.append(doc)
            f_ts_start += dt.timedelta(hours=1)                 
        if f_ts_start > f_ts_end:
            break
    target_coll = db_personal[collection_name]
    target_data = list(target_coll.find({}, {"_id": 0}))
    for doc in corrected_data:
        if doc in target_data:
            pass
        else:
            print('adding document: ', doc)
            # target_coll.insert_one(doc)
    print("done")
    return 'done'

@app.route('/impute/<collection_name>')
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
            # pers_coll.update_one({"_id": doc["_id"]}, {"$set": {"generated": doc_ref["generated"]}})
            print('updated: ', doc)
    print('done')
    return 'done'

@app.route('/plot/<collection_name>')
def plot_coll(collection_name):
    pers_coll = db_personal[collection_name]
    data = list(pers_coll.find({}, {"_id": 0}))
    type = data[0]['type']
    data = sorted(data, key=lambda x: x["timestamp"])
    x = [dt.datetime.strptime(_['timestamp'], '%Y-%m-%d %H:%M:%S') for _ in data]
    y = [float(_['generated']) for _ in data]
    plt.plot(x, y)
    plt.xlabel('Timestamp')
    plt.ylabel('Generated Values')
    plt.title(f'Plot for {collection_name}, type {type}')
    plt.xticks(rotation=45)
    plt.show()
    return 'done'

# this one structures the db like formentera's
# @app.route('/api/regeneratedb')
# def regenerate_gokc_db():
#     metersreal = []
#     responseToken = requests.put('https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/generate-token',
#                                     json={'UserCode': 'vpp4islands.webservice', 'Password': 'kYJ95zg3'},
#                                     headers={'Content-Type': 'application/json'})
#     token = json.loads(responseToken.content.decode('utf8'))
#     responseMeters = requests.post(
#                 'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetCustomerPortalSubscriptions',
#                 json={'PageNumber': 1, 'PageSize': 100000},
#                 headers={'Content-Type': 'application/json', 'aril-service-token': token})
#     allSubscriptions = json.loads(json.dumps(responseMeters.json()))
#     filtered = allSubscriptions.get('ResultList', '0')
#     for singlesub in filtered:
#         metersreal.append(singlesub.get('SubscriptionSerno', '0'))
#     print('meters count: ', len(metersreal))
#     # watch out
#     metersreal = metersreal[0:2]
#     result = []
#     for meter in metersreal:
#         response_consumption = requests.post(
#                         'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetOwnerConsumptions',
#                         json={'OwnerSerno': meter, 'StartDate': '20011001130000', 'EndDate': '205001010100000',
#                             'WithoutMultiplier': 'false', 'MergeResult': 'false'},
#                         headers={'Content-Type': 'application/json', 'aril-service-token': token})
#         data = json.loads(json.dumps(response_consumption.json()))
#         meter_data = []
#         for pd in data['InConsumption']:
#             meter_data.append({'timestamp_end': pd['pd'], 'consumption': pd['cn']})
#         first_nest = {'meter': meter, 'meter_data': meter_data}
#         result.append(first_nest)
#     return jsonify(result)

def missing_timestamps_func(data):
    sorted_data = sorted(data, key=lambda x : x['timestamp_end'])
    ts_start = sorted_data[0]['timestamp_end']
    f_ts_start = dt.datetime.strptime(str(ts_start), '%Y%m%d%H%M%S')
    ts_end = sorted_data[-1]['timestamp_end']
    f_ts_end = dt.datetime.strptime(str(ts_end), '%Y%m%d%H%M%S')
    counter = 0
    missing_timestamps = []
    for doc in data:    
        doc_ts = dt.datetime.strptime(str(doc['timestamp_end']), '%Y%m%d%H%M%S')
        if str(f_ts_start) > str(f_ts_end):
            return missing_timestamps
        if str(doc_ts) != str(f_ts_start):
            while str(doc_ts) != str(f_ts_start):
                info_missing = {"meter": str(doc['meter']), "timestamp_end": str(f_ts_start)}
                missing_timestamps.append(info_missing)
                f_ts_start += dt.timedelta(hours=1)
                counter += 1
        if str(doc_ts) == str(f_ts_start):
            f_ts_start += dt.timedelta(hours=1)
                        
@app.route('/api/regeneratedb')
def regenerate_gokc_db():
    metersreal = []
    responseToken = requests.put('https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/generate-token',
                                    json={'UserCode': 'vpp4islands.webservice', 'Password': 'kYJ95zg3'},
                                    headers={'Content-Type': 'application/json'})
    token = json.loads(responseToken.content.decode('utf8'))
    responseMeters = requests.post(
                'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetCustomerPortalSubscriptions',
                json={'PageNumber': 1, 'PageSize': 100000},
                headers={'Content-Type': 'application/json', 'aril-service-token': token})
    allSubscriptions = json.loads(json.dumps(responseMeters.json()))
    filtered = allSubscriptions.get('ResultList', '0')
    for singlesub in filtered:
        metersreal.append(singlesub.get('SubscriptionSerno', '0'))
    print('meters count: ', len(metersreal))
    # watch out for this. has to be removed before finalization
    metersreal = metersreal[:10]
    result = []
    for meter in metersreal:
        response_consumption = requests.post(
                        'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetOwnerConsumptions',
                        json={'OwnerSerno': meter, 'StartDate': '20011001130000', 'EndDate': '205001010100000',
                            'WithoutMultiplier': 'false', 'MergeResult': 'false'},
                        headers={'Content-Type': 'application/json', 'aril-service-token': token})
        data = json.loads(json.dumps(response_consumption.json()))
        for pd_inc, pd_outc in zip(data['InConsumption'], data['OutConsumption']):
                inc_value = pd_inc['cn']
                outc_value = pd_outc['cn']
                to_append = {'meter': meter, 'timestamp_end': pd_inc['pd'], 'InConsumption': inc_value, 'OutConsumption': outc_value}
                result.append(to_append)
    print('got data from api')
    list_of_types = list(db_prod['listgokcsm'].find({}, {'_id': 0}))
    to_db = []
    grouped_data = {}
    # missing_timestamps = missing_timestamps_func(result)
    for item in result:
        timestamp_end = item['timestamp_end']
        type_str = ''
        for type in list_of_types:
            if item['meter'] == type['meter']:
                type_str = type['type']
                if type_str == 'sub_sm':
                    type_str = 'secsub_sm'
        if timestamp_end not in grouped_data:
            input_string = str(timestamp_end)
            input_datetime = datetime.strptime(input_string, "%Y%m%d%H%M%S")
            one_hour = dt.timedelta(hours=1)
            new_datetime = input_datetime - one_hour
            timestamp_start = new_datetime.strftime("%Y%m%d%H%M%S")
            grouped_data[timestamp_end] = {'timestamp_end': str(timestamp_end), 'timestamp_start': timestamp_start, 'meters': []}
        grouped_data[timestamp_end]['meters'].append({
            'type': type_str,
            'meter': int(item['meter']),
            'InConsumption': float(item['InConsumption']),
            'OutConsumption': float(item['OutConsumption'])
        })
    for timestamp, data in grouped_data.items():
        to_db.append(data)
    sorted_res = sorted(to_db, key=lambda x: x['timestamp_end'])
    print('to db len: ', len(sorted_res))
    # gokc_smartmeters
    gokc_sm_coll = db_personal['gokc_smartmeters']
    gokc_data_db = list(gokc_sm_coll.find({}, {'_id': 0}))
    for doc in sorted_res:
        existing_doc = next((item for item in gokc_data_db if item["timestamp_end"] == doc["timestamp_end"]), None)
        if existing_doc:
            update_fields = {}
            for key, value in doc.items():
                if key != "timestamp_end" and existing_doc.get(key) != value:
                    update_fields[key] = value
            if update_fields:
                gokc_sm_coll.update_one({"timestamp_end": doc["timestamp_end"]}, {"$set": update_fields})
                print('Updating document with ts:', doc['timestamp_end'])
            else:
                print('No differences found for document with ts:', doc['timestamp_end'])
        else:
            gokc_sm_coll.insert_one(doc)
            print('Inserting document with ts:', doc['timestamp_end'])
    return 'Done'
    
if __name__ == "__main__":
    print('App served on port 9999')
    serve(app, host='0.0.0.0', port=9999)
