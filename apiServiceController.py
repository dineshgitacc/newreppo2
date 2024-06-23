import logging
import requests
from requests.auth import HTTPBasicAuth
from django.conf import settings

# Get an instance of a logging
log = logging.getLogger(__name__)

class ApiServiceController:
   
    def request_basic_auth(self, url, username, password):

        try:           
            try:
                result = []
                print(url)
                # url = "https://dev59626.service-now.com/api/now/table/sys_db_object?sysparm_fields=sys_id,sys_name,label,name"
                # username ="admin"
                # password ="tWScDMeN7pz2"
                response_api = requests.get(url, 
                            auth=HTTPBasicAuth(username, password))
                if response_api.status_code == 200:
                    result = response_api.json()
                    return {"error_code": 2, "result":result['result']}
                else:   
                    return {"error_code": 1, "error_msg": "error n connection" }
            except requests.exceptions.Timeout as timeout:
                # Maybe set up for a retry, or continue in a retry loop
                return {"error_code": 1, "error_msg": str(timeout) }
            except requests.exceptions.TooManyRedirects as t_err:
                # Tell the user their URL was bad and try a different one
                return {"error_code": 1, "error_msg": str(t_err) }
            except requests.exceptions.RequestException as re:
                # catastrophic error. bail.
                return {"error_code": 1, "error_msg": str(re) }         
        except Exception as e:
            log.error(e)
            return {"error_code": 1, "error_msg": str(e) }
        except KeyError as err:
            log.error(err)
            return {"error_code": 1, "error_msg": str(err) }

    def request_access_token(self, url, params):
        try:
            try:
                result = []
                headers = {
                    'Content-type': 'application/x-www-form-urlencoded'
                }
                access_token_result = requests.post(url, headers=headers, data=params, verify=False)
                if access_token_result.status_code == 200:
                    result = access_token_result.json()
                    return {"error_code": 2, "result":result}
                else:
                    return {"error_code": 1, "error_msg": "Invalid credentials" }
            except requests.exceptions.Timeout as timeout:
                # Maybe set up for a retry, or continue in a retry loop
                return {"error_code": 1, "error_msg": str(timeout) }
            except requests.exceptions.TooManyRedirects as t_err:
                # Tell the user their URL was bad and try a different one
                return {"error_code": 1, "error_msg": str(t_err) }
            except requests.exceptions.RequestException as re:
                # catastrophic error. bail.
                return {"error_code": 1, "error_msg": str(re) }
        except Exception as e:
            log.error(e)
            return {"error_code": 1, "error_msg": str(e) }
        except KeyError as err:
            log.error(err)
            return {"error_code": 1, "error_msg": str(err) }

    def request_oauth(self, url, access_token):
        try:
            try:
                result = []
                if access_token:
                    headers = {
                        'Content-type': 'application/json',
                        'Authorization': 'Bearer %s' % access_token
                    }
                    api_result = requests.get(url, headers=headers, verify=False)
                    if api_result.status_code == 200:
                        result = api_result.json()
                        return {"error_code": 2, "result":result}
                    else:
                        return {"error_code": 1, "error_msg": "error in connection" }
                else:
                    return {"error_code": 1, "error_msg": "error in instance connection" }
            except requests.exceptions.Timeout as timeout:
                # Maybe set up for a retry, or continue in a retry loop
                return {"error_code": 1, "error_msg": str(timeout) }
            except requests.exceptions.TooManyRedirects as t_err:
                # Tell the user their URL was bad and try a different one
                return {"error_code": 1, "error_msg": str(t_err) }
            except requests.exceptions.RequestException as re:
                # catastrophic error. bail.
                return {"error_code": 1, "error_msg": str(re) }
        except Exception as e:
            log.error(e)
            return {"error_code": 1, "error_msg": str(e) }
        except KeyError as err:
            log.error(err)
            return {"error_code": 1, "error_msg": str(err) }
