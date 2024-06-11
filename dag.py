from django.conf import settings
import datetime
import logging
import json
from django.conf import settings
import requests
from workflow.models import MLAlgorithmDetail
from workflow.serializers import MLAlgorithmDetailSerializer
from mlmodel.models import MLTrainingMaster
from datapipe.models import DataMapManual, FileMaster, MLAlgorithm, StatusEnum, DataMap, DataMapFilter, DataMapFieldMapping
from datapipe.serializers import DataMapManualSerializer, DataMapSerializer, DataMapFilterSerializer, FileMasterSerializer

from connection.models import Connection, ConnectionType
from connection.serializers import ConnectionSerializer, ConnectionTypeSerializer

# Get an instance of a logging
log = logging.getLogger(__name__)


def _generate_payload(data_map_id, dag_id, dag_status, schedule_interval, workflow_tasks, ml_data):
    """
    Generate payload data for airflow dags creation
    :param data_map_id:
    :param dag_id:
    :param schedule_interval:
    :param workflow_tasks:
    :return:
    """
    log.info("Generating payload for {}".format(data_map_id))
    conf_payload = {"dag_id": dag_id,
                    "dag_status": dag_status,
                    "schedule_interval": schedule_interval,
                    "tags": ["opexwise", "auto", "workflow", data_map_id],
                    "tasks": workflow_tasks,
                    "ml_algorithm_details": ml_data,
                    "workflow_algorithm_callback" : settings.API_URL + "/workflow/algorithm/callback/"
                    }
    payload = {"conf": {"conf": conf_payload, "data_map_id": data_map_id}}
    log.info("Generated Payload is {}".format(payload))
    return payload


def _trigger_dag(payload):
    """
    Make Airflow API Trigger Dag ( create_dag )
    :param payload:
    :return:
    """
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        log.info("Received Payload is {}".format(payload))
        create_dag_url = settings.AIRFLOW_HOST + settings.AIRFLOW_DAGRUN_ENDPOINT
        log.info(create_dag_url)
        resp = requests.post(create_dag_url, headers=headers, data=json.dumps(payload),
                             auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASSWORD))
        log.info(resp)
        return resp
    except Exception as e:
        print(e)
        print(create_dag_url)
        print("Exception occurred while calling Airflow")
        log.error("Exception occurred while calling Airflow")
        log.error(create_dag_url)
        log.error(payload)
        log.error(e)
    return None


def _generate_payload_for_mlalgorithm(data_map_id, dag_id, file_name, algorithm_name, label, is_train_or_test):
    """
    Generate payload data for airflow dags for ML Algorithm
    :param data_map_id:
    :param dag_id:
    :param file_name:
    :param algorithm_name:
    :param label:
    :param is_train_or_test:
    :return:
    """
    log.info("Generating payload for ML is {}".format(data_map_id))
    conf_payload = {"dag_id": dag_id,
                    "file_name": file_name,
                    "algorithm_name": algorithm_name,
                    "labels": label,
                    "is_train_or_test": is_train_or_test}
    payload = {"conf": conf_payload}
    log.info("Generated Payload for ML is {}".format(payload))
    return payload

def _trigger_dag_for_mlalgorithm(payload, dag_id):
    """
    Make Airflow API Trigger Dag ( update_dag )
    :param payload:
    :return:
    """
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        log.info("Received Payload for ML is {}".format(payload))
        update_dag_url = settings.AIRFLOW_HOST + settings.AIRFLOW_ENDPOINT + dag_id + "/dagRuns"
        log.info(update_dag_url)
        resp = requests.post(update_dag_url, headers=headers, data=json.dumps(payload),
                             auth=(settings.AIRFLOW_USER, settings.AIRFLOW_PASSWORD))
        log.info(resp)
        return resp
    except Exception as e:
        print(e)
        print(update_dag_url)
        print("Exception occurred while calling Airflow for ML Algorithm")
        log.error("Exception occurred while calling Airflow for ML Algorithm")
        log.error(update_dag_url)
        log.error(payload)
        log.error(e)
    return None


def _file_extensions(val):
    """
    Include File extenaion, Allow multiple extenaion
    :param val:
    :return: List
    """
    ext_types = []
    if val == 'excel':
        ext_types = ['.xlsx', '.xlsm', '.xlsb', '.xltx', '.xltm', '.xls', '.xlt', '.xla', '.xlw', '.xlr']
    elif val == 'csv':
        ext_types = ['.csv']
    return ext_types


def _format_filter(filter_dict):
    """
    Formar filter keys
    :param filter_dict:
    :return:
    """
    _final_filter = {}
    for key, value in filter_dict.items():
        key = key.replace('source_', '').replace('destination_', '').strip()
        if key == 'file_format':
            key = 'file_extensions'
            value = _file_extensions(value)
        elif key == 'recevied_date':
            key = 'received_date'
            if value is None or value == '':
                value = datetime.datetime.now().strftime("%Y-%m-%d")
        _final_filter[str(key.strip())] = value
    return _final_filter


def _analysis_columns(data_map_id):
    """
    Prepare Analysis Columns
    :param data_map_id:
    :return:
    """
    sql = 'SELECT 1 as data_map_field_mapping_id,dfm.data_field_master_id,dfm.field_name as displayName,' \
          'dfm.field_backend_name as column,dfm.field_backend_type as dataType FROM data_map_field_mapping as dmfm ' \
          'JOIN data_field_master as dfm ON dfm.data_field_master_id=dmfm.source_field_id  WHERE data_map_id={} AND ' \
          'destination_field_id=0'.format(data_map_id)
    data_columns = DataMapFieldMapping.objects.raw(sql.strip())
    analysis_columns = []
    for data_column in data_columns:
        analysis_columns.append({"headerName": data_column.column, "displayName": data_column.displayname,
                                 "dataType": data_column.datatype})
    return analysis_columns

def _file_upload_columns(data_map_id):
    """
    Prepare Analysis Columns
    :param data_map_id:
    :return:
    """
    sql = 'SELECT 1 as data_map_field_mapping_id,dfm.data_field_master_id,dfm.field_name as displayName,' \
          'dfm.field_backend_name as column,dfm.field_backend_type as dataType FROM data_map_field_mapping as dmfm ' \
          'JOIN data_field_master as dfm ON dfm.data_field_master_id=dmfm.source_field_id  WHERE data_map_id={} AND ' \
          'destination_field_id=0'.format(data_map_id)
    data_columns = DataMapFieldMapping.objects.raw(sql.strip())
    analysis_columns = []
    for data_column in data_columns:
        analysis_columns.append({"field_name": data_column.displayname,"field_type": data_column.datatype, "field_backend_name": data_column.column})
    return analysis_columns


def _analysis_fields(data_map_id):
    """
    Prepare Analysis Fields
    :param data_map_id:
    :return:
    """
    allowable_analysis_fields = ['clusterAnalysisFields',
                                 'contentClass',
                                 'solutionField',
                                 'dateFields',
                                 'dataAnalysis',
                                 'analysisColumns',
                                 'textAnalysisColumn']
    data_map_fields = DataMapFieldMapping.objects.raw(
        'SELECT 1 as data_map_field_mapping_id, dfm.field_name as field_value,ddfm.field_backend_name as field_key '
        'FROM data_map_field_mapping as dmfm JOIN data_field_master as dfm ON '
        'dfm.data_field_master_id=dmfm.source_field_id JOIN data_field_master as ddfm ON '
        'ddfm.data_field_master_id=dmfm.destination_field_id WHERE data_map_id={}'.format(data_map_id))
    analysis_fields = {}
    for analysis_field in allowable_analysis_fields:
        analysis_fields[analysis_field] = []
    for data_field in data_map_fields:
        if data_field.field_key in allowable_analysis_fields:
            analysis_fields[data_field.field_key].append(data_field.field_value)
    return analysis_fields


def _manual_upload_file_credentials():
    """
    Imap credentials for manual file upload
    :return:
    """
    return {
        "hostname": str(settings.MANUAL_EMAIL_HOST),
        'port': str(settings.MANUAL_EMAIL_PORT),
        'username': str(settings.MANUAL_EMAIL_HOST_USER),
        'password': str(settings.MANUAL_EMAIL_HOST_PASSWORD)
    }


def create_dag(data_map_id):
    """
    Create Dag in Airflow
    :param data_map_id:
    :return:
    """
    log.info("Getting data map object for {}".format(data_map_id))
    # Get all questions
    data_map_obj = DataMap.objects.get(data_mdata_map_idap_id=data_map_id)
    data_map_serializer = DataMapSerializer(data_map_obj)
    data_map = data_map_serializer.data

    # ML algorithm data:
    ml_algorithm_name = ""
    ml_algorithm_code = ""
    model_name = ""
    label = []
    ms_sql_select_query_data = ''
    if "ms_sql_select_query_data" in data_map['extras'][0]:
        if data_map['extras'][0]['ms_sql_select_query_data']:
            ms_sql_select_query_data = data_map['extras'][0]['ms_sql_select_query_data']
    if "ml_algorithms_model_details" in data_map['extras'][0]:
        ml_model_details = data_map['extras'][0]['ml_algorithms_model_details']

        if ml_model_details['algorithm_id']:
            ml_algorithm_id = ml_model_details['algorithm_id']
            algorithm_details = MLAlgorithm.objects.get(algorithm_id = str(ml_algorithm_id))
            ml_algorithm_name = algorithm_details.algorithm_name
            ml_algorithm_code =  algorithm_details.algorithm_code
            if ml_algorithm_id == 1:
                label = []
                if ml_model_details['ml_model_id']:
                    ml_model_id = str(ml_model_details['ml_model_id'])
                    ml_model_details = MLTrainingMaster.objects.get(ml_training_id = ml_model_id)
                    model_name = ml_model_details.training_model_name
            elif  ml_algorithm_id == 5 or  ml_algorithm_id == 7:
                model_name = ""
                if ml_model_details['description']:
                    description = ml_model_details['description']
                    label = description.split(',')
                    label = list(map(str.strip, label))
            elif ml_algorithm_id == 2:
                model_name = ""
                label = []
    
    ml_data  = {
        "ml_algorithm_name" : ml_algorithm_name,
        "ml_algorith_code" : ml_algorithm_code,
        "model_name": model_name,
        "label": label
    }
    log.info("ML Data - {} ".format(ml_data))
    
    table_name = data_map['table_name']
    schedule_interval = data_map['schedule']
    dag_id = data_map['table_name']
    dag_status = StatusEnum(data_map['status']).name.upper()
    data_map_filter_obj = DataMapFilter.objects.get(data_map_id=data_map_id)
    data_map_filter_serializer = DataMapFilterSerializer(data_map_filter_obj)
    data_map_filter = data_map_filter_serializer.data
    log.info("Table name {}".format(table_name))
    # Prepare source and destination
    data_map_source = _file_upload_columns(data_map_id)
    data_map_filter['source_file_fieldsource_filter_values'][''] = data_map_source
    data_map_filter['analysis_custom_query'] = ms_sql_select_query_data

    conn_list = [
        {"type": "SOURCE", "id": data_map['source_connection_id'], "filters": data_map_filter['source_filter_values']},
        {"type": "DESTINATION", "id": data_map['destination_connection_id'],
         "filters": data_map_filter['destination_filter_values']}
    ]
    log.info(conn_list)
    workflow_tasks = []
    for conn in conn_list:
        # prepare necessary fields ( connection, connection_type )
        _connection_type = conn['type']
        connection_obj = Connection.objects.get(connection_id=conn['id'])
        connection_serializer = ConnectionSerializer(connection_obj)
        connection = connection_serializer.data
        login_params = connection['parameter']
        credentials = json.loads(login_params)
        connection_type_obj = ConnectionType.objects.get(connection_type_id=connection['connection_type_id'])
        connection_type_serializer = ConnectionTypeSerializer(connection_type_obj)
        connection_type_data = connection_type_serializer.data
        application_code = connection_type_data['application_code']

        if application_code == 'fileupload':
            credentials = _manual_upload_file_credentials()
        # need to restrict analysis_table_name, analysis_fields, analysis_columns for only supportfirst analysis
        task = {'credentials': credentials,
                'filters': _format_filter(conn['filters']),
                'application_code': application_code,
                'connection_type': _connection_type,
                'analysis_custom_query' : ms_sql_select_query_data
                }
        if application_code == 'supportfirst' or application_code == 'superset':
            task['analysis_table_name'] = table_name
            task['analysis_fields'] = _analysis_fields(data_map_id)
            task['analysis_columns'] = _analysis_columns(data_map_id)
            task['analysis_fields']['sentimentAnalysisColumn'] = []
            task['analysis_fields']['summarizationColumn'] = []
            task['analysis_fields']['predictiveAnalysisColumn'] = []
            task['analysis_fields']['predictiveAnalysisType'] = []
            task['analysis_fields']['predictive_analysis_destination'] = []
            task['analysis_fields']['predictiveAnalysisAutoMLType'] = []
            task['analysis_fields']['forecast_date_field'] = []

            if "extras" in data_map:
                if data_map['extras'][0]:
                    if "sentiment_analysis" in data_map['extras'][0]:
                        if data_map['extras'][0]["sentiment_analysis"]:
                            task['analysis_fields']['sentimentAnalysisColumn'].append(data_map['extras'][0]['sentiment_analysis'])
                    if "summarization" in data_map['extras'][0]:
                        if data_map['extras'][0]["summarization"]:
                            task['analysis_fields']['summarizationColumn'].append(data_map['extras'][0]['summarization'])
                    if "predictive_analysis" in data_map['extras'][0]:
                        if "predictive_analysis" in data_map['extras'][0]['predictive_analysis']:
                            task['analysis_fields']['predictiveAnalysisColumn'] = data_map['extras'][0]['predictive_analysis']['predictive_analysis']
                        if "predectionType" in data_map['extras'][0]['predictive_analysis']:                            
                            task['analysis_fields']['predictiveAnalysisType'] = [data_map['extras'][0]['predictive_analysis']['predectionType']]
                        if "predictive_analysis_destination" in data_map['extras'][0]['predictive_analysis']:
                            task['analysis_fields']['predictive_analysis_destination'] = data_map['extras'][0]['predictive_analysis']['predictive_analysis_destination']  
                        if "predectionAnalysisType" in data_map['extras'][0]['predictive_analysis']:
                            task['analysis_fields']['predictiveAnalysisAutoMLType'] = data_map['extras'][0]['predictive_analysis']['predectionAnalysisType']  
                        if "forecast_date_field" in data_map['extras'][0]['predictive_analysis']:
                            task['analysis_fields']['forecast_date_field'] = [data_map['extras'][0]['predictive_analysis']['forecast_date_field']]  
                              

        if application_code == 'fileupload' and "file_name" not in task: 
            task['file_name'] = get_file_name(data_map_id)
        workflow_tasks.append(task)
    try:
        log.info("Generate Payload")
        # generate payload data for airflow dags
        payload = _generate_payload(data_map_id, dag_id, dag_status, schedule_interval, workflow_tasks, ml_data)
        trigger_dag_response = _trigger_dag(payload)
        log.info(trigger_dag_response)
        return {"status_code": trigger_dag_response.status_code, "error": False}
    except Exception as e:
        return {"status_code": 500, "error": True, "error_message": str(e)}

    
def get_file_name(data_map_id):
    try:
        log.info("Datamap id - {}".format(data_map_id))
        datamap_manual = DataMapManual.objects.filter(data_map_id=data_map_id).order_by('data_map_manual_id').last()
        datamap_manual_serializer = DataMapManualSerializer(datamap_manual)
        file_name = ""
        log.info("Datamap manual - "+str(datamap_manual_serializer.data))
        if datamap_manual_serializer and datamap_manual_serializer.data["file_id"]:
            file_id = datamap_manual_serializer.data["file_id"]
            file_details = FileMaster.objects.get(file_id=file_id)
            file_serializer = FileMasterSerializer(file_details)
            file_name = file_serializer.data["file_name"]

        log.info("file name - {}".format(file_name))
        return file_name
    except Exception as e:
        log.error(e)
        return {"status_code": 500, "error": True, "error_message": str(e)}

def create_dag_ml(data_map_id):
    """
    Create Dag in Airflow
    :param data_map_id:
    :return:
    """
    try:
        log.info("Update dag with ML Algorithm")
        log.info("Getting data map object for {}".format(data_map_id))
        label_list = ""
        is_train_or_test = 'train'
        ml_algorithm_detail_obj = MLAlgorithmDetail.objects.filter(data_map_id=data_map_id).order_by('algorithm_detail_id').last()
        ml_algorithm_detail_serializer = MLAlgorithmDetailSerializer(ml_algorithm_detail_obj)
        ml_algorithm_detail = ml_algorithm_detail_serializer.data
        # generate payload for alrflow dags for ML Algorithm
        log.info(ml_algorithm_detail)
        dag_id = ml_algorithm_detail['table_name']
        file_name = ml_algorithm_detail['file_name']
        algorithm_name = ml_algorithm_detail['algorithm_name']
        label = ml_algorithm_detail['label'][0]['label']

        if label:
            label_list = label.split(',')
            label_list = list(map(str.strip, label_list))
        if ml_algorithm_detail['is_training'] == 1:
            is_train_or_test = 'train'
        else:
            is_train_or_test = 'test'
        log.info("Generate Payload for ml")
        payload_for_ml = _generate_payload_for_mlalgorithm(data_map_id, dag_id, file_name, algorithm_name, label_list, is_train_or_test)
        trigger_dag_manual_response = _trigger_dag_for_mlalgorithm(payload_for_ml, dag_id)
        log.info(trigger_dag_manual_response)
        return {"status_code": trigger_dag_manual_response.status_code, "error": False}
    except Exception as e:
        log.error("Exception in Airflow while creating ML Algorithm for data_map_id - {}".format(data_map_id))
        log.error(e)
        return {"status_code": 500, "error": True, "error_message": str(e)}

def trigger_workflow(data_map_id):
    """
    Trigger Dag in Airflow
    :param data_map_id:
    :return:
    """
    try:
        log.info("Getting data map object for {}".format(data_map_id))
        log.info("Trigger airflow")
        data_map_obj = DataMap.objects.get(data_map_id=data_map_id)
        data_map_serializer = DataMapSerializer(data_map_obj)
        data_map = data_map_serializer.data
        
        dag_id = data_map['table_name']

        payload_for_ml = {"conf":{}}
        trigger_dag_manual_response = _trigger_dag_for_mlalgorithm(payload_for_ml, dag_id)
        log.info(trigger_dag_manual_response.status_code)
        log.info(trigger_dag_manual_response)
        return {"status_code": trigger_dag_manual_response.status_code, "error": False}

    except Exception as e:
        log.error("Exception while triggering airflow for data_map_id - {}".format(data_map_id))
        log.error(e)
        return {"status_code": 500, "error": True, "error_message": str(e)}
