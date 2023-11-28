import requests
import json
from pymongo import MongoClient, UpdateOne, InsertOne
import datetime as dt
from datetime import datetime
import time
import os

connection_string=os.getenv('connection_string')
personal_connection_string = os.getenv('personal_connection_string')
conn_prod_db = MongoClient(connection_string, 27017)
conn_personal_db = MongoClient(personal_connection_string, 27017)

db_prod = conn_prod_db['vpp4i']
db_personal = conn_personal_db['vpp4_database_test']

def timer_function(func):
    def wrapper(*args, **kwargs):
        t_0 = time.time()
        result = func(*args, **kwargs)
        t_1 = time.time()
        total_time = t_1 - t_0
        hours, remainder = divmod(total_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            print(f"Function {func.__name__} took {int(hours)} hours, {int(minutes)} minutes, {seconds:.2f} seconds")
        elif minutes > 0:
            print(f"Function {func.__name__} took {int(minutes)} minutes, {seconds:.2f} seconds")
        else:
            print(f"Function {func.__name__} took {seconds:.6f} seconds")
        return result
    return wrapper

# @timer_function
# def regenerate_gokc_db(meter_index_start=0, meter_index_end=5, start_date='20011001130000',end_date='205001010100000'):
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
#     metersreal = metersreal[meter_index_start:meter_index_end]
#     result = []
#     for meter in metersreal:
#         response_consumption = requests.post(
#                         'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetOwnerConsumptions',
#                         json={'OwnerSerno': meter, 'StartDate': start_date, 'EndDate': end_date,
#                             'WithoutMultiplier': 'false', 'MergeResult': 'false'},
#                         headers={'Content-Type': 'application/json', 'aril-service-token': token})
#         data = json.loads(json.dumps(response_consumption.json()))
#         for pd_inc, pd_outc in zip(data['InConsumption'], data['OutConsumption']):
#                 inc_value = pd_inc['cn']
#                 outc_value = pd_outc['cn']
#                 to_append = {'meter': meter, 'timestamp_end': pd_inc['pd'], 'InConsumption': inc_value, 'OutConsumption': outc_value}
#                 result.append(to_append)
#     print('got data from api')
#     list_of_types = list(db_prod['listgokcsm'].find({}, {'_id': 0}))
#     to_db = []
#     grouped_data = {}
#     # missing_timestamps = missing_timestamps_func(result)
#     for item in result:
#         timestamp_end = item['timestamp_end']
#         type_str = ''
#         for type in list_of_types:
#             if item['meter'] == type['meter']:
#                 type_str = type['type']
#                 if type_str == 'sub_sm':
#                     type_str = 'secsub_sm'
#         if timestamp_end not in grouped_data:
#             input_string = str(timestamp_end)
#             input_datetime = datetime.strptime(input_string, "%Y%m%d%H%M%S")
#             one_hour = dt.timedelta(hours=1)
#             new_datetime = input_datetime - one_hour
#             timestamp_start = new_datetime.strftime("%Y%m%d%H%M%S")
#             grouped_data[timestamp_end] = {'timestamp_end': str(timestamp_end), 'timestamp_start': timestamp_start, 'meters': []}
#         grouped_data[timestamp_end]['meters'].append({
#             'type': type_str,
#             'meter': int(item['meter']),
#             'InConsumption': float(item['InConsumption']),
#             'OutConsumption': float(item['OutConsumption'])
#         })
#     for timestamp, data in grouped_data.items():
#         to_db.append(data)
#     # here fill missing timestamps for meter with 0.0 ?
#     sorted_res = sorted(to_db, key=lambda x: x['timestamp_end'])
#     print('to db len: ', len(sorted_res))
#     gokc_sm_coll = db_personal['gokc_smartmeters'] # after testing is completed this should be changed to production db
#     gokc_data_db = list(gokc_sm_coll.find({}, {'_id': 0}))
#     for doc in sorted_res:
#         existing_doc = next((item for item in gokc_data_db if item["timestamp_end"] == doc["timestamp_end"]), None)
#         if existing_doc:
#             existing_meters = set(meter["meter"] for meter in existing_doc['meters'])
#             missing_meters = [meter for meter in doc['meters'] if meter["meter"] not in existing_meters]
#             if missing_meters:
#                 gokc_sm_coll.update_one({"timestamp_end": doc["timestamp_end"]}, {"$addToSet": {"meters": {"$each": missing_meters}}})
#                 print('Updating document with ts:', doc['timestamp_end'])
#             else:
#                 # print('Document with ts:', doc['timestamp_end'], ' already has all the meters')
#                 continue
#         else:
#             gokc_sm_coll.insert_one(doc)
#             print('Inserting document with ts:', doc['timestamp_end'])
#     print('Done with metersreal[', meter_index_start, ':', meter_index_end, ']')
#     return

@timer_function
def get_data_api_and_insert(token, metersreal, meters_start, meters_end, meters_len, step):
    # print(f'Fetching data for metersreal[{meters_start}:{meters_end}]')
    result = []
    for meter in metersreal[meters_start:meters_end]:
        try:
            print('meter: ', meter)
            response_consumption = requests.post(
                            'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetOwnerConsumptions',
                            json={'OwnerSerno': meter, 'StartDate': '20221001130000', 'EndDate': '20231020150000',
                                'WithoutMultiplier': 'false', 'MergeResult': 'false'},
                            headers={'Content-Type': 'application/json', 'aril-service-token': token})
            data = json.loads(json.dumps(response_consumption.json()))
            for pd_inc, pd_outc in zip(data['InConsumption'], data['OutConsumption']):
                    inc_value = pd_inc['cn']
                    outc_value = pd_outc['cn']
                    to_append = {'meter': meter, 'timestamp_end': pd_inc['pd'], 'InConsumption': inc_value, 'OutConsumption': outc_value}
                    result.append(to_append)
            print('Received data from uedas /api')
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
            # here fill missing timestamps for meter with 0.0 ?
            sorted_res = sorted(to_db, key=lambda x: x['timestamp_end'])
            print('To db len:', len(sorted_res))
            gokc_sm_coll = db_personal['historic_gokc_smartmeters'] # after testing is completed this should be changed to production db
            gokc_data_db = list(gokc_sm_coll.find({}, {'_id': 0}))
            print('received gokc data already on db for checking')
            bulk_operations = []
            for doc in sorted_res:
                existing_doc = next((item for item in gokc_data_db if item["timestamp_end"] == doc["timestamp_end"]), None)
                if existing_doc:
                    existing_meters = set(meter["meter"] for meter in existing_doc['meters'])
                    missing_meters = [meter for meter in doc['meters'] if meter["meter"] not in existing_meters]
                    if missing_meters:
                        bulk_operations.append(
                            UpdateOne({"timestamp_end": doc["timestamp_end"]}, {"$addToSet": {"meters": {"$each": missing_meters}}})
                        )
                        print('Updating document with ts:', doc['timestamp_end'])
                    else:
                        continue
                else:
                    bulk_operations.append(InsertOne(doc))
                    print('Inserting document with ts:', doc['timestamp_end'])
            if bulk_operations:
                print('Performing bulk operations...')
                gokc_sm_coll.bulk_write(bulk_operations)
            elif bulk_operations == []:
                print('No operation needed')
            # print(f'Done with metersreal[{meters_start}:{meters_end}]')
            # break # this was not good, except for the print statement above...
        except Exception as e:
            print('Error: ', e)
            if meters_start == 0:
                meters_start = 0
                meters_end = meters_start + step
            else:
                meters_start -= step
                if meters_end == meters_len:
                    meters_end = meters_len
                else:
                    meters_end -= step

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
        data_db = list()
    meters_len = len(metersreal)
    step = 146
    for i in range(0, meters_len, step):
        meters_start = i
        meters_end = i + step
        if meters_end >= meters_len:
            meters_end = meters_len
        get_data_api_and_insert(token, metersreal, meters_start, meters_end, meters_len, step)
    print('ALL done :)')
    return

regenerate_gokc_db()