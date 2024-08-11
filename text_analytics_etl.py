import logging
import json
from datetime import datetime

from airflow.api.client.local_client import Client
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.enqueue_trigger import trigger_enqueue_with_microseconds
from helper.metadata_without_error import get_metadata_without_error
from helper.utils import _cluster_logging
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base
from supportfirst.create_table import CreateTable
from supportfirst.load_data_main_table import LoadData
from supportfirst.load_temp_table import LoadTempTable
from sqlalchemy.inspection import inspect

post_gres_hook = PostgresHook(postgres_conn_id="postgresopex")
engine = post_gres_hook.get_sqlalchemy_engine()

SUPER_SET_QUEUE = Variable.get("SUPER_SET_QUEUE_NAME", default_var="SUPER_SET_DEV")
NLP_QUEUE = Variable.get("NLP_QUEUE_NAME", default_var="NLP_DEV_LOCAL")
TEMP_TABLE = Variable.get("TEMP_TABLE", default_var="tmp_table")
ROOT_PATH = Variable.get("ROOT_PATH", default_var="/opt/airflow/")
SCHEMA = Variable.get("SCHEMA", default_var="public")

data_type_mapping = {
    "Text": "text",
    "smallint": "smallint",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "integer": "integer",
    "character varying": "varchar",
    "date": "date",
    "time": "time"
}

def delete_table(table_name):
    """Drop the given table"""
    base = declarative_base()
    metadata = get_metadata_without_error(engine)
    table = metadata.tables.get(table_name)
    if table is not None:
        logging.info(f'Deleting {table_name} table')
        base.metadata.drop_all(engine, [table], checkfirst=True)


def validate_input(**kwargs):
    print(kwargs)
    cluster_job_ids = kwargs['params']['cluster_job_ids'] if 'cluster_job_ids' in kwargs['params'] else {}
    _logging = _cluster_logging(kwargs, cluster_job_ids)
    current_file_name = kwargs['params']['downloaded_file']
    table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
    var_name = table_name + "_process_check"
    #Checking if the file is processed for the table
    processed_file = Variable.get(var_name, default_var = "")
    print("file that was previously processed",processed_file)
    print("current file name",current_file_name)
    print("is_processed = ",processed_file == current_file_name)
    if processed_file == current_file_name:
        _logging.skipped("File already processed")
        raise AirflowSkipException("File already processed")   
    else:
        # if "my_sql_files" or "postgresql_files" in current_file_name: #as per requirement for my sql and postgres we need to drop the main table before appending new data to the same workflow
        #     delete_table(table_name) 
        return 'CreateOrAlterTable'
    

def create_or_alter_table(**kwargs):
    """Create Table"""
    print(kwargs)
    if 'cluster_job_ids' not in kwargs['params']:
            print("cluster job ids not listed.")
    cluster_job_ids = kwargs['params']['cluster_job_ids'] if 'cluster_job_ids' in kwargs['params'] else {}
    _logging = _cluster_logging(kwargs, cluster_job_ids)
    try:
        table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
        # Connection to database
        pg_inspector = inspect(engine)

        table_names = pg_inspector.get_table_names(schema=SCHEMA)
        if table_name not in table_names:
            kwargs['ti'].xcom_push(key='{}_min'.format(table_name), value=1)

        text_analysis_column = kwargs['params']["analysis_input"]\
            ["analysis_fields"]["textAnalysisColumn"]
        data_fields = kwargs['params']["file_field"]
        create_table_obj = CreateTable(table_name, data_fields, text_analysis_column,_logging)
        create_table_obj.add_column()
        create_table_obj.create_table()
        _logging.info("Successfully created table in superset")
    except Exception as e:
        _logging.error("Job failed. Please contact support")
        raise AirflowFailException("Job failed. Please contact support")


def load_tmp_table(**kwargs):
    """Creates and loads file content to temp table"""
    if 'cluster_job_ids' not in kwargs['params']:
            print("cluster job ids not listed.")
    cluster_job_ids = kwargs['params']['cluster_job_ids'] if 'cluster_job_ids' in kwargs['params'] else {}
    _logging = _cluster_logging(kwargs, cluster_job_ids)
    try:
        table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
        file_path = kwargs['params']['downloaded_file']
        data_fields = kwargs['params']["file_field"]
        text_analysis_column = kwargs['params']["analysis_input"]\
            ["analysis_fields"]["textAnalysisColumn"]
        data_map_id = kwargs['params']["data_map_id"]
        load_table_obj = LoadTempTable(
            "{}_{}".format(TEMP_TABLE, table_name), ROOT_PATH,
            file_path, data_fields,
            text_analysis_column,
            data_map_id,table_name,
            _logging)
        load_table_obj.load_file_to_dataframe()
        load_table_obj.validate()
        load_table_obj.load_table()
    except Exception as e:
        _logging.error("Job failed. Please contact support")
        raise AirflowFailException("Job failed. Please contact support")
               

def load_data_query(**kwargs):
    """Inserts rows from temp table to the newly created table"""
    if 'cluster_job_ids' not in kwargs['params']:
            print("cluster job ids not listed.")
    cluster_job_ids = kwargs['params']['cluster_job_ids'] if 'cluster_job_ids' in kwargs['params'] else {}
    _logging = _cluster_logging(kwargs, cluster_job_ids)
    try:
        table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
        data_fields = kwargs['params']["file_field"]
        analysis_columns = kwargs['params']["analysis_input"]["analysis_columns"]
        text_analysis_column = kwargs['params']["analysis_input"]\
            ["analysis_fields"]["textAnalysisColumn"]
        load_data_obj = LoadData(table_name, data_fields, analysis_columns, text_analysis_column, _logging)
        load_data_obj.prepare_column()
        load_data_obj.load_data()
        #load_data_obj.add_index()
    except Exception as e:
        _logging.error("Job failed. Please contact support")
        raise AirflowFailException("Job failed. Please contact support")


def rm_tmp_table(**kwargs):
    table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
    table_name_tmp = "{}_{}".format(TEMP_TABLE, table_name)
    table_name += "_process_check"
    file_path = kwargs['params']['downloaded_file']
    Variable.set(table_name, file_path)
    delete_table(table_name=table_name_tmp)


def truncate_table(**kwargs):
    """Delete rows from the table"""
    try:
        table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
    except KeyError:
        raise AirflowFailException("Table name not provided")
    base = declarative_base()
    metadata = get_metadata_without_error(engine)
    table = metadata.tables.get(table_name)
    if table is not None:
        query = table.delete()
        with engine.connect() as con:
            con.execute(query)

def super_set_call(**kwargs):
    c = Client(None, None)
    run_id = datetime.utcnow().strftime('opexwise_%Y-%m-%dT%H:%M:%S.%f')
    cluster_job_ids = kwargs['params']['cluster_job_ids']
    table_name = kwargs['params']["analysis_input"]["analysis_table_name"]
    conf = {
        "queue": SUPER_SET_QUEUE,
        "cluster_job_ids" : cluster_job_ids,
        "reference_id" : kwargs['params']["data_map_id"],
        "table_name" : table_name,
        "key" : run_id
    }
    trigger_enqueue_with_microseconds(conf)
                


def process_end(**kwargs):
    cluster_job_ids = kwargs['params']['cluster_job_ids'] if 'cluster_job_ids' in kwargs['params'] else {}
    _logging = _cluster_logging(kwargs, cluster_job_ids)
    send_to_nlp = kwargs["params"]["analysis_input"]["analysis_fields"]["textAnalysisColumn"]
    if send_to_nlp:
        table_name = kwargs["params"]["analysis_input"]["analysis_table_name"]
        metadata = get_metadata_without_error(engine)
        destination_table_name = metadata.tables[table_name]
        cluster_job_ids = kwargs['params']['cluster_job_ids']
        run_id = datetime.utcnow().strftime('opexwise_%Y-%m-%dT%H:%M:%S.%f')
        conf = {
            "queue": NLP_QUEUE,
            "cluster_job_ids" : cluster_job_ids,
            "key" : "nlp_queue_data",
            "table_name" : kwargs["params"]["analysis_input"]["analysis_table_name"],
            "content" : kwargs["params"]["analysis_input"]["analysis_fields"]["textAnalysisColumn"],
            "content_class" : kwargs["params"]["analysis_input"]["analysis_fields"]["contentClass"],
            "reference_id" : kwargs['params']["data_map_id"],
            "solution_field" : kwargs["params"]["analysis_input"]["analysis_fields"]["solutionField"],
            "params": kwargs['params']
        }
        print(conf)
        trigger_enqueue_with_microseconds(conf)
        return True
    _logging.info("NLP is skipped")
    return
    
