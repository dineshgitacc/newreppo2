import pandas as pd
import json
import logging
import urllib3
import os

from django.conf import settings
from connection.models import Connection
from datapipe.serializers import ConnectionViewSerializer
from datapipe.controller.api_service.apiServiceController import ApiServiceController

urllib3.disable_warnings()
logging.captureWarnings(True)
# Get an instance of a logging
log = logging.getLogger(__name__)


class SalesforceController:
    def api_name(self, request):

        try:
            log.info("SalesforceController api_name")
            log.info(request.data)
            getconntion_type = Connection.objects.filter(connection_id=request.data['connection_id'])
            getconntion_serializer = ConnectionViewSerializer(getconntion_type, many=True)

            if not getconntion_serializer.data:
                return {"error": True, "message": 'Invalid connection'}
            print(getconntion_serializer.data)

            print(type(getconntion_serializer.data[0]['parameter']))
            if isinstance(getconntion_serializer.data[0]['parameter'], str):
                json_load = json.loads(getconntion_serializer.data[0]['parameter'])
            else:
                json_load = getconntion_serializer.data[0]['parameter']
            instance_url = os.path.join(json_load['url'],settings.SALESFORCE_API_NAME)
            login_url = json_load['login_url']
            params = {
                'username': json_load['username'],
                'password': json_load['password'],
                'client_id': json_load['client_id'],
                'client_secret': json_load['client_secret'],
                'grant_type': json_load['grant_type'],
                'redirect_url': json_load['redirect_url']
            }
            service_cal = ApiServiceController()
            api_response = service_cal.request_access_token(login_url, params)
            if api_response['error_code'] == 2:
                if "access_token" in api_response['result'] and api_response['result']['access_token']:
                    access_token = api_response['result']['access_token']
                    response = service_cal.request_oauth(instance_url, access_token)
                    if response['error_code'] == 2:
                        response_data = []
                        if response['result']:
                            df = pd.DataFrame(response['result']['sobjects'])
                            df.insert(4, "api_category_name", df['label'], True)
                            df.insert(5, "api_category_value", df['name'], True)
                            df = df[['api_category_name','api_category_value']]
                            response_data = {'api_details': df.to_dict(orient='records')}
                        return {"error": False, "message": 'Success', "data": response_data}
                    else:
                        return {"error": True, "message": response['error_msg']}
                else:
                    return {"error": True, "message": api_response['error_msg']}
            else:
                return {"error": True, "message": api_response['error_msg']}

        except Exception as e:
            log.error(e)
            return {"error": True, "message": str(e)}
        except KeyError as err:
            log.error(err)
            return {"error": True, "message": str(err)}

    def api_parameter(self, request):

        try:
            log.info("SalesforceController api_parameter")
            log.info(request.data)
            getconntion_type = Connection.objects.filter(connection_id=request.data['connection_id'])
            getconntion_serializer = ConnectionViewSerializer(getconntion_type, many=True)

            if not getconntion_serializer.data:
                return {"error": True, "message": 'Invalid connection'}
            print(getconntion_serializer.data)

            print(type(getconntion_serializer.data[0]['parameter']))
            if isinstance(getconntion_serializer.data[0]['parameter'], str):
                json_load = json.loads(getconntion_serializer.data[0]['parameter'])
            else:
                json_load = getconntion_serializer.data[0]['parameter']
            instance_url = os.path.join(json_load['url'],settings.SALESFORCE_API_NAME)
            if "api_category_name" in request.data and request.data['api_category_name']:
                instance_url += "/" + request.data['api_category_name'] + settings.SALESFORCE_API_PARAMETER
            login_url = json_load['login_url']
            params = {
                'username': json_load['username'],
                'password': json_load['password'],
                'client_id': json_load['client_id'],
                'client_secret': json_load['client_secret'],
                'grant_type': json_load['grant_type'],
                'redirect_url': json_load['redirect_url']
            }
            service_cal = ApiServiceController()
            api_response = service_cal.request_access_token(login_url, params)
            if api_response['error_code'] == 2:
                if "access_token" in api_response['result'] and api_response['result']['access_token']:
                    access_token = api_response['result']['access_token']
                    response = service_cal.request_oauth(instance_url, access_token)
                    if response['error_code'] == 2:
                        response_data = []
                        if response['result']:
                            df = pd.DataFrame(response['result']['fields'])
                            df.insert(2, "field_name", df['label'], True)
                            df.insert(3, "field_value", df['name'], True)
                            df.insert(3, "field_type", df['type'], True)

                            for s_key, s_values in settings.SERVICENOW_DATATYPE.items():
                                df['field_type'] = df['field_type'].replace(s_values, s_key)

                            df = df[['field_name', 'field_value', 'field_type']]

                            # df = df[['field_name']]
                            response_data = {'api_details': [{"api_source_parameters": df.to_dict(orient='records')}]}
                        return {"error": False, "message": 'Success', "data": response_data}
                    else:
                        return {"error": True, "message": response['error_msg']}
                else:
                    return {"error": True, "message": api_response['error_msg']}
            else:
                return {"error": True, "message": api_response['error_msg']}

        except Exception as e:
            log.error(e)
            return {"error": True, "message": str(e)}
        except KeyError as err:
            log.error(err)
            return {"error": True, "message": str(err)}
