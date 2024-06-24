import logging
import requests
import json
import urllib3
import socket
from django.conf import settings


from google.cloud import bigquery
from google.oauth2 import service_account

urllib3.disable_warnings()
logging.captureWarnings(True)
# Get an instance of a logging
log = logging.getLogger(__name__)

class DatabricksoauthConnection:
    def __init__(self) -> None:
        pass

    def test_connection(self,param):
        try:
            if param:
                
                param = json.loads(param)
                print("checking")
                

                flag ,response=self.make_connection(param)
                

                if flag:
                    if response.status_code == 200:
                        # self.save_response_to_file(response.json())
                        return (True,"Connection successfully established")
                    
                    else:
                        return (False,"Connection failed")
                else:
                    return (False ,response)

        except Exception as e:
            log.error(e)
            return (False, str(e))
        

    def make_connection(self,param):
        try:

            databricks="https://dbc-df1b446c-aa5f.cloud.databricks.com/"
            
            access_token="dapi8fa52bff08521d4a694c07310888d5bf"

            cluster_id="0620-095103-6p61kc1w"

# endpoint="api/2.0/jobs/list"
            endpoint="api/2.0/sql/statements"
# endpoint="api/2.0/workspace/list"
# endpoint="api/2.0/clusters/get"
# endpoint="api/2.0/commands/execute"
            details={
                "url": param["url"],
                "cluster_id": param["project_id"],
                "warehouse_id": param['private_key_id'],
                "access_token":param["refresh_token"]
                
            }
            # print(details)

            sql_query='select * from arlo_opexwise.default.sf_ticket_raw_31_may_02_jun limit 1'
            # sql_query='select * from samples.nyctaxi.trips limit 3'



            url=f"{databricks}{endpoint}"

            headers={
                "Authorization":"Bearer "+access_token,
                "Content-Type":"application/json"
                    }

            body={
                "statement":sql_query,
                "warehouse_id":"7408e6a350cb9b2a"
            }
# body={
#     "laguage":"sql",
#     "clusterId":cluster_id,
#     "contextId":"default",
#     "command":sql_query
# }

            # payload={
            #     "clusted_id":cluster_id
            # } 

            session = requests.Session()
            session.verify = False


            response=requests.post(url,headers=headers,data=json.dumps(body),verify=False)
# response=requests.get(url,headers=headers,params=payload)
# response=requests.get(url,headers=headers)
            # print(response.json().items())

            if response.status_code == 200:
                log.info("connection successfull")
                # log.debug(response.json())
                # execution_id=response.json().get("statement_id")
                # print(execution_id)
                # result_url = f"{databricks}/api/2.0/sql/queries/{execution_id}/results"
                # result_response=requests.get(result_url,headers=headers,verify=False)
                # result_data=result_response.json()
                # log.info("connection successful")
                execution_id = response.json().get("statement_id")
                result_url = f"{databricks}/api/2.0/sql/queries/{execution_id}/results"
                result_response = requests.get(result_url, headers=headers, verify=False)
                result_data = result_response.json()

                # Extracting column names
                columns = result_data['schema']['columns']
                column_names = [column['name'] for column in columns]

                # Extracting column values
                rows = result_data['data']
                column_values = []
                for row in rows:
                    values = row['cells']
                    column_values.append(values)

                print("Column Names:", column_names)
                print("Column Values:", column_values)

                return True, response
            else:
                print("Not connected", response.status_code)
                return False, response 
                        
        except Exception as e:

            log.error(e)

            return (False,str(e))

   