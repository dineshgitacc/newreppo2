import pandas as pd
import json
import logging
import urllib3
import requests
import os

from django.conf import settings
from connection.models import Connection
from datapipe.serializers import ConnectionViewSerializer
from datapipe.controller.api_service.apiServiceController import ApiServiceController

urllib3.disable_warnings()
logging.captureWarnings(True)
# Get an instance of a logging
log = logging.getLogger(__name__)


class DatabricksController:
    def api_name(self, request):

        try:
            log.info("Databricks api_name")
            log.info(request.data)
            getconnection_type = Connection.objects.filter(connection_id=request.data['connection_id'])
            getconnection_serializer = ConnectionViewSerializer(getconnection_type, many=True)

            if not getconnection_serializer.data:
                return {"error": True, "message": 'Invalid connection'}
            # print(getconnection_serializer.data)

            print(type(getconnection_serializer.data[0]['parameter']))
            if isinstance(getconnection_serializer.data[0]['parameter'], str):
                json_load = json.loads(getconnection_serializer.data[0]['parameter'])
            else:
                json_load = getconnection_serializer.data[0]['parameter']

            # sql_query="SELECT * FROM  system.information_schema.tables  WHERE table_catalog = 'arlo_opexwise'"
            # sql_query = "SELECT * FROM ARLO_OPEXWISE.INFORMATION_SCHEMA.TABLES "
            sql_query="SELECT * FROM  information_schema.tables  WHERE table_catalog = 'arlo_opexwise'"   

                                
                               
                               
            payload = {
                "warehouse_id": json_load["private_key_id"],
                "statement":sql_query,
                
            }
            # print(json_load)
            # login_response = requests.post(json_load["url"], json=payload)
            token_header = {
                "Authorization": "Bearer " + json_load["refresh_token"]
            }
            # payload = {}
            endpoint="api/2.0/sql/statements"

            url=json_load["url"]+endpoint
            # url="https://dbc-df1b446c-aa5f.cloud.databricks.com/explore/data"
            # print(url)
            
            response = requests.post(url, data=json.dumps(payload), headers=token_header,verify=False)

            response_json = response.json()
            print("xxxxxxxxxxxxxxxxxxxxxxxxxx")
            print(response_json.values())
            print("xxxxxxxxxxxxxxxxxxxx")
            # json_object = json.dumps(response_json, indent=4)
            # with open("sample3.json", "w") as outfile:
            #     outfile.write(json_object)
            # print("before into the response true")
            if response_json['status']['state'] == 'SUCCEEDED':
                # print("get into the response true")
                # if response_json['code'] == 200:
                rows = response_json['result']['data']
                if rows:
                    column_names = rows[0]['schema']['fields']
                    values = [row['data'] for row in rows]
                    df = pd.DataFrame(values, columns=column_names)
                    print(df)
                    # Process the DataFrame as needed
                    # response_json['table_name'] = 'Databricks'
                    # df = pd.DataFrame([response_json])
                    # df.insert(1, "api_category_name", df['table_name'], True)
                    # df.insert(2, "api_category_value", df['table_name'], True)
                    # df = df[['api_category_name', 'api_category_value']]
                    # df = df[df.api_category_name != '']
                    if not df.empty:
                        response_data = {'api_details': df.to_dict(orient='records')}
                        print(response_data)
                        return {"error": False, "message": 'Success', "data": response_data}
                    else:
                        return {"error": True, "message": "Table not found"}
                    
        except Exception as e:
            log.error(e)
            return {"error": True, "message": str(e)}
        except KeyError as err:
            log.error(err)
            return {"error": True, "message": str(err)}
        
    def api_parameter(self, request):

        try:
            log.info("Databricks api_name")
            log.info(request.data)
            getconnection_type = Connection.objects.filter(connection_id=request.data['connection_id'])
            getconnection_serializer = ConnectionViewSerializer(getconnection_type, many=True)

            if not getconnection_serializer.data:
                return {"error": True, "message": 'Invalid connection'}
            print(getconnection_serializer.data)

            print(type(getconnection_serializer.data[0]['parameter']))
            if isinstance(getconnection_serializer.data[0]['parameter'], str):
                json_load = json.loads(getconnection_serializer.data[0]['parameter'])
            else:
                json_load = getconnection_serializer.data[0]['parameter']

            table_name = request.data['api_category_name']    
            sql_query = "SELECT DISTINCT(COLUMN_NAME),DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '"+table_name+"'"
            log.info(sql_query)
            payload = {
                "warehouse_id": json_load["private_key_id"],
                "statement":sql_query,
                
            }
            token_header = {
                "Authorization": "Bearer " + json_load["refresh_token"]
            }
            # payload = {}
            endpoint="api/2.0/sql/statements"
            url=json_load["url"]+endpoint
            # payload = {}
            response = requests.post(url, data=json.dumps(payload), headers=token_header,verify=False)
            response_json = response.json()
            print(response_json)
            
            if response_json['status']['state'] == 'SUCCEEDED':
                if 'rows' in response_json:
                    response_data=[]
                    columns = [column['name'] for column in response_json['rows'][0]['schema']['columns']]
                    data_types = [column['type_name'] for column in response_json['rows'][0]['schema']['columns']]

                    df = pd.DataFrame(list(zip(columns, data_types)), columns=['field_name', 'field_type'])
                    df.insert(1, "field_value", df['field_name'], True)

                    for s_key, s_values in settings.DATABRICKS_DATATYPE.items():
                        df['field_type'] = df['field_type'].replace(s_values, s_key)

                    df = df[df.field_name != '']
                    if not df.empty:
                        response_data = {'api_details': [{ "api_source_parameters":df.to_dict(orient='records')}]}
                        return {"error": False, "message": 'Success', "data": response_data}
                    else:
                        return {"error": True, "message": "No Columns Found"}
                else:
                    return {"error": True, "message": "'rows' key not found in the response"}
            else:
                return {"error": True, "message": "API request failed"}
            
        except Exception as e:
            log.error(e)
            return {"error": True, "message": str(e)}
        except KeyError as err:
            log.error(err)
            return {"error": True, "message": str(err)}
    