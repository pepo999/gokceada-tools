from pymongo import MongoClient
from flask import Flask, jsonify, request
from waitress import serve
import datetime as dt
import requests
import json
import time

app = Flask(__name__)

conn_prod_db = MongoClient('mongodb://diegomary:Atreius%4062@vpp4i.nevergoback.cloud:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256', 27017)
db_prod = conn_prod_db['vpp4i']

def missing_timestamps_func(data, ts_start, ts_end):
    sorted_data = sorted(data, key=lambda x : x['timestamp'])
    # ts_start = sorted_data[0]['timestamp']
    f_ts_start = dt.datetime.strptime(str(ts_start), '%Y%m%d%H%M%S')
    # ts_end = sorted_data[-1]['timestamp']
    f_ts_end = dt.datetime.strptime(str(ts_end), '%Y%m%d%H%M%S')
    counter = 0
    missing_timestamps = []
    print('f_ts_start ', ts_start)
    for doc in sorted_data:    
        doc_ts = dt.datetime.strptime(str(doc['timestamp']), '%Y%m%d%H%M%S')
        if str(f_ts_start) > str(f_ts_end):
            return missing_timestamps
        if str(doc_ts) != str(f_ts_start):
            while str(doc_ts) != str(f_ts_start):
                info_missing = {"timestamp": str(f_ts_start)}
                missing_timestamps.append(info_missing)
                f_ts_start += dt.timedelta(hours=1)
                counter += 1
        if str(doc_ts) == str(f_ts_start):
            f_ts_start += dt.timedelta(hours=1)

@app.route('/getgokcfromto')
def get_gokc_from_to():
    start_ts = request.json.get('timestamp_start')
    end_ts = request.json.get('timestamp_end')
    metersreal = []
    missing_ts = []
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
    result = []
    meters_len = len(metersreal)
    step = 50
    type = "node_uedas"
    gokcgrid_list = list(db_prod['gokcgrid'].find({"type": type}, {"_id": 0}))
    sub_sms = [x['meter'] for x in gokcgrid_list]
    for i in range(0, meters_len, step): 
        try:       
            meters_start = i
            meters_end = i + step
            if meters_end >= meters_len:
                meters_end = meters_len  
            print(f'trying with metersreal[{meters_start}:{meters_end}]')
            for meter in metersreal[meters_start:meters_end]:
                if meter in sub_sms:
                    response_consumption = requests.post(
                                    'https://osos.uedas.com.tr/aril-portalserver/customer-rest-api/proxy-aril/GetOwnerConsumptions',
                                    json={'OwnerSerno': meter, 'StartDate': start_ts, 'EndDate': end_ts,
                                        'WithoutMultiplier': 'false', 'MergeResult': 'false'},
                                    headers={'Content-Type': 'application/json', 'aril-service-token': token})
                    data = json.loads(json.dumps(response_consumption.json()))
                    meter_data = []
                    for pd_inc, pd_outc in zip(data['InConsumption'], data['OutConsumption']):
                        inc_value = pd_inc['cn']
                        outc_value = pd_outc['cn']
                        to_append = {'timestamp': str(pd_inc['pd']), 'InConsumption': inc_value, 'OutConsumption': outc_value}
                        meter_data.append(to_append) 
                    doc = {"meter": meter, "type": type,"data": meter_data}
                    result.append(doc)
                else:
                    continue
            print(f'received data for metersreal[{meters_start}:{meters_end}] from uedas /API')
        except Exception as e:
            print('error: ', e)
            if meters_start == 0:
                meters_start = 0
                meters_end = meters_start + step
            else:
                meters_start -= step
                if meters_end == meters_len:
                    meters_end = meters_len
                else:
                    meters_end -= step
            print('meters_start in except ', meters_start)
            print('meters_end in except ', meters_end)
    print('meters data from uedas /API obtained succesfully')
    boxes_data = []
    boxes_coll_names = [
                    "box1_gokc",
                    "box2_gokc",
                    "box3_gokc"
                    ]
    for box_coll_name in boxes_coll_names:
        box_coll = db_prod[box_coll_name]
        datetime_start = dt.datetime.strptime(start_ts, '%Y%m%d%H%M%S')
        box_start = datetime_start.strftime('%Y-%m-%d %H:%M:%S')
        datetime_end = dt.datetime.strptime(end_ts, '%Y%m%d%H%M%S')
        box_end = datetime_end.strftime('%Y-%m-%d %H:%M:%S')
        box_data_db = list(box_coll.find({"timestamp": {"$gte": box_start, "$lt": box_end}}, {"_id":0}))
        type = ''
        box_data = []
        for _ in box_data_db:
            input_ts = str(_['timestamp'])
            dt_object = dt.datetime.strptime(input_ts, "%Y-%m-%d %H:%M:%S")
            timestamp_f = dt.datetime.strftime(dt_object, "%Y%m%d%H%M%S")
            doc_corr = {"timestamp": timestamp_f, "generated": _['generated']}
            type = _['type']
            box_data.append(doc_corr)
        missing_box_ts = missing_timestamps_func(box_data, start_ts, end_ts)
        if missing_box_ts != []:   
            missing_ts.append({"name":box_coll_name, "missing": missing_box_ts})
        boxes_data.append({"box": type, "name": box_coll_name, "data": box_data})
    print('boxes data obtained succesfully')
    resp = {"meters": result, "boxes": boxes_data}
    print('meters len ', len(result))  
    print('boxes len', len(boxes_data))
    print(missing_ts)
    return jsonify(resp)

if __name__ == "__main__":
    print('App served on port 9999')
    serve(app, host='0.0.0.0', port=9999)