"""
This module contains a SupportfirstV2 Hook which allows you to connect to your SupportfirstV2 instance,
retrieve data from it, and write that data to a file for other uses.
"""
import logging
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote
import requests
import pandas as pd
import os
from airflow.hooks.base import BaseHook
from airflow.models import Variable

log = logging.getLogger(__name__)


def _correct_path(name: str, local_output_directory: str) -> str:
    is_exists = os.path.exists(local_output_directory)
    if not is_exists:
        # Create a new directory because it does not exist
        os.makedirs(local_output_directory)

    return (
        local_output_directory + name
        if local_output_directory.endswith('/')
        else local_output_directory + '/' + name
    )


def _check_file_exist(data_file_name: str, local_output_directory: str) -> str:
    data_file_path = os.path.join(local_output_directory, data_file_name)
    is_exists = os.path.exists(data_file_path)
    print(is_exists)
    return is_exists


class Supportfirstv2Hook:
    def __init__(self, credentials):
        self.credentials = credentials
        self.df = None

    def make_connection(self):
        try:
            payload = {
                "clientId": self.credentials["client_id"],
                "userName": self.credentials["username"],
                "password": self.credentials["password"]
            }
            response = requests.post(self.credentials["login_url"], json=payload)
            if response.status_code == 200:
                return response
            else:
                return None
        
        except Exception as e:        
            logging.error(e)
            return None
        
    def pull_data(self, table_name, local_output_directory, file_name, column, kwargs):
        dag_id = kwargs["dag"].dag_id
        data_store_file_name = 'data_store.csv'
        no_of_processed_data = int(Variable.get("data_tacker_for_{}".format(dag_id), default_var = 0))
        print("no_of_process",no_of_processed_data)
        
        credentials = kwargs['credentials']
        login_response = self.make_connection()
        token_header = {
            "Authorization": "Bearer " + login_response.json()["data"]["access_token"]
        }
        payload = {}
        response = requests.post(credentials["url"], json=payload, headers=token_header)
        data = response.json()
        self.df = pd.DataFrame(data['data'])

        check_file_exist = _check_file_exist(data_store_file_name, local_output_directory)

        if not check_file_exist:
            no_of_rows_in_dataframe = len(self.df)
            self.df.to_csv(_correct_path(file_name, local_output_directory), index = False)
            self.df.to_csv(os.path.join(local_output_directory, data_store_file_name), index=False)
            print("Successful in saving",file_name)
            print("Successful in saving",data_store_file_name)

            no_of_processed_data += no_of_rows_in_dataframe        
            Variable.set("data_tacker_for_{}".format(dag_id) , no_of_processed_data)
            print("no of processed data after pulling",no_of_processed_data)
    
            return no_of_rows_in_dataframe
        
        else:
            self.df.to_csv(_correct_path(file_name, local_output_directory), index = False)
            print("Successful in saving",file_name)

            df1 = pd.read_csv(os.path.join(local_output_directory, data_store_file_name))
            df2 = pd.read_csv(_correct_path(file_name, local_output_directory))

            common_columns = list(set(df1.columns) & set(df2.columns))
            merge_df = pd.merge(df1,df2, on=common_columns, how='inner')
            incremental_data = df2[~df2.index.isin(merge_df.index)]

            incremental_data.to_csv(_correct_path(file_name, local_output_directory), header=True, index=False)  
            incremental_data.to_csv(os.path.join(local_output_directory, data_store_file_name), mode='a', header=False, index=False)
        
            self.df = pd.read_csv(_correct_path(file_name, local_output_directory))

            no_of_rows_in_dataframe = len(self.df)
            print(no_of_rows_in_dataframe)
            no_of_processed_data += no_of_rows_in_dataframe        
            Variable.set("data_tacker_for_{}".format(dag_id) , no_of_processed_data)
            print("no of processed data after pulling",no_of_processed_data)
    
            return no_of_rows_in_dataframe


    
    # def pull_data(self, table_name, local_output_directory, file_name, coloumn, kwargs):
    #     dag_id = kwargs["dag"].dag_id
    #     credentials = kwargs['credentials']
    #     login_response = self.make_connection()
    #     token_header = {
    #         "Authorization": "Bearer " + login_response.json()["data"]["access_token"]
    #     }
    #     payload = {}
    #     response = requests.post(credentials["url"], json=payload, headers=token_header)
    #     data = response.json()
    #     self.df = pd.DataFrame(data['data'])
    #     no_of_rows_in_dataframe = len(self.df)
    #     self.df.to_csv(_correct_path(file_name, local_output_directory), index = False)
    #     print("Successful in saving",file_name)     
    #     return no_of_rows_in_dataframe
