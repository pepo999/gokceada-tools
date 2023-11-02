import pandas as pd
from pymongo import MongoClient

conn_prod_db = MongoClient('mongodb://diegomary:Atreius%4062@vpp4i.nevergoback.cloud:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256', 27017)
prod_db = conn_prod_db['vpp4i']
prod_coll = prod_db['listgokcsm']
conn_personal_db = MongoClient("mongodb+srv://pietroviglino999:rkoEZiZp6tzduEUZ@vpp4dbtest.yseft60.mongodb.net/?retryWrites=true&w=majority", 27017)
db_personal = conn_personal_db['vpp4_database_test']
nodes_coll = db_personal['nodes_no_uedas_gokc']

xc_data = pd.read_excel('nodes_lines_gokceada_Oct31_2023.xlsx', sheet_name='nodes')
sub_sm_list = list(prod_coll.find({"type": "sub_sm"}, {"_id": 0}))
meters_sub = [int(x['meter']) for x in sub_sm_list]

sernos = xc_data['Gokçeada grid nodes'].iloc[2:].tolist()
sernos = list(set(sernos))
longitudes = xc_data['Unnamed: 5'].iloc[2:].tolist()
latitudes = xc_data['Unnamed: 6'].iloc[2:].tolist()

ser_lat_long = []
for x, y, z in zip(sernos, latitudes, longitudes):
    new_doc = {"meter": x, "lat": y, "long": z, "type": "sub_sm_no_uedas"}
    ser_lat_long.append(new_doc)

result_list = [x for x in ser_lat_long if x['meter'] not in meters_sub]

# def db_insert(list):
#     for doc in list:
#         nodes_coll.insert_one(doc) 
#     print('Done')  

xc_data_lines = pd.read_excel('nodes_lines_gokceada_Oct31_2023.xlsx', sheet_name='lines')
xc_data_lines = xc_data_lines.iloc[2:, :2]
print(xc_data_lines)
list_of_tuples = list(xc_data_lines.to_records(index=False))
reversed_list = [(b, a) for a, b in list_of_tuples]
total_links = list_of_tuples + reversed_list


meter_links = []

for meter in meters_sub:
    links = []
    for tuple_value in total_links:
        if meter == tuple_value[0]: 
            second_value = tuple_value[1]
            # print(f"Found match: {meter} is the first value, and the second value is {second_value}")
            links.append(second_value)
        else:
            # print(f"No match found for {meter}")
            continue
    doc = {"meter": meter, "link_to": links} 
    meter_links.append(doc)
 
def update_collection_with_links(data_list):
    for item in data_list:
        meter_value = item['meter']
        link_to_value = item['link_to']
        document = nodes_coll.find_one({'meter': meter_value})
        if document:
            nodes_coll.update_one(
                {'_id': document['_id']},
                {'$set': {'link_to': link_to_value}}
            )

# needs testing. check again before testing

# update_collection_with_links(meter_links)
