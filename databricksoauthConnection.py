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
                    if response.code == 200:
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
            # param=param

            databricks="https://dbc-df1b446c-aa5f.cloud.databricks.com/"

            access_token="dapi8fa52bff08521d4a694c07310888d5bf"
            cluster_id="0620-095103-6p61kc1w"

# endpoint="api/2.0/jobs/list"
            endpoint="api/2.0/sql/statements"
# endpoint="api/2.0/workspace/list"
# endpoint="api/2.0/clusters/get"
# endpoint="api/2.0/commands/execute"

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


            response=requests.post(url,headers=headers,data=json.dumps(body))
# response=requests.get(url,headers=headers,params=payload)
# response=requests.get(url,headers=headers)

            if response.status_code == 200:
                print("success")
                print(response.json())
                print(response.text)
                print("data fetched")
            else:
                print("not connected",response.status_code)    
                        
        except Exception as e:

            log.error(e)

            return (False,str(e))

   