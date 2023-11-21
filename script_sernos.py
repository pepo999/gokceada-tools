import pandas as pd
from pymongo import MongoClient
import os

connection_string=os.getenv('connection_string')
personal_connection_string = os.getenv('personal_connection_string')

conn_prod_db = MongoClient(connection_string, 27017)
prod_db = conn_prod_db['vpp4i']
prod_coll = prod_db['gokcgrid']
conn_personal_db = MongoClient(personal_connection_string, 27017)
db_personal = conn_personal_db['vpp4_database_test']
personal_coll = db_personal['gokcgrid']

xc_data_nodes = pd.read_excel('nodes_lines_gokceada_Oct31_2023.xlsx', sheet_name='nodes')

sernos = xc_data_nodes['Gokçeada grid nodes'].iloc[2:].tolist()
longitudes = xc_data_nodes['Unnamed: 5'].iloc[2:].tolist()
latitudes = xc_data_nodes['Unnamed: 6'].iloc[2:].tolist()

ser_lat_long = []
for x, y, z in zip(sernos, latitudes, longitudes):
    type = ''
    first_2 = str(x)[:2]
    if first_2 == '90':
        type = 'node_no_uedas'
    if first_2 == '91':
        type = 'virtual_node'
    elif first_2 != '90' and first_2 != '91':
        type = 'node_uedas'
    new_doc = {"meter": x, "lat": y, "long": z, "type": type}
    ser_lat_long.append(new_doc)

for _ in ser_lat_long:
    if _['meter'] == 910097:
        assert _['lat'] == 40.2054, 'Lat is mismatched'
    if _['meter'] == 900186:
        assert _['lat'] == 40.1235012227, 'Lat is mismatched'
        assert _['long'] == 25.6912829562, 'Long is mismatched'   

xc_data_lines = pd.read_excel('nodes_lines_gokceada_Oct31_2023.xlsx', sheet_name='lines')
xc_data_lines = xc_data_lines.iloc[2:, :2]
# print(xc_data_lines)
list_of_tuples = list(xc_data_lines.to_records(index=False))
reversed_list = [(b, a) for a, b in list_of_tuples]
total_links = list_of_tuples + reversed_list

meter_links = []

for doc in ser_lat_long:
    links = []
    for tuple_value in total_links:
        if doc['meter'] == tuple_value[0]: 
            second_value = tuple_value[1]
            # print(f"Found match: {doc['meter']} is the first value, and the second value is {second_value}")
            links.append(second_value)
        else:
            # print(f"No match found for {doc['meter]}")
            continue
    doc = {"meter": doc['meter'], "lat": doc['lat'], "long": doc['long'], "type": doc['type'], "link_to": links} 
    meter_links.append(doc)
 
# def update_collection_with_links(data_list):
#     for item in data_list:
#         meter_value = item['meter']
#         link_to_value = item['link_to']
#         document = prod_coll.find_one({'meter': meter_value})
#         if document:
#             prod_coll.update_one(
#                 {'_id': document['_id']},
#                 {'$set': {'link_to': link_to_value}}
#             )

def fill_coll_db(data):
    for doc in data:
        print('adding doc: ', doc)
        prod_coll.insert_one(doc)
    print('done')

def add_for_type(type):
    prod_db = conn_prod_db['vpp4i']
    listgokc_coll = prod_db['listgokcsm']
    private_sms = list(listgokc_coll.find({"type": type}, {"_id":0}))
    for doc in private_sms:
        print('adding doc ', doc)
        prod_coll.insert_one(doc)
    print('done')
    
    
    
