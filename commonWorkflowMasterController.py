import logging
import json
from django.conf import settings
import datetime
import copy
from django.db import connection
from django.db.models.query_utils import Q
from deepdiff import DeepDiff
from rest_framework.response import Response
from rest_framework import status

from workflow.serializers import AirflowTriggerDagSerializer, MLAlgorithmDetailCreateSerializer, MLAlgorithmDetailUpdateSerializer, WorkflowConnectionListDetailsSerializer, WorkflowMasterSerializer, WorkflowConnectionSerializer, WorkflowDataMapSerializer, WorkflowHistorySerializer
from workflow.models import AirflowTriggerDag, MLAlgorithmDetail, WorkflowHistory, WorkflowMaster, WorkflowConnection, WorkflowDataMap
from datapipe.models import DataMap, StatusEnum
from connection.models import Connection, ConnectionType
from datapipe.serializers import DataMapSerializer
from airflow.dag import create_dag, create_dag_ml, trigger_workflow

# Get an instance of a logging
log = logging.getLogger(__name__)

class CommonWorkflowMasterController:

    def field_validation(data):
        error_validate = 2
        error_msg = ""
        if "workflow_name" not in data :
            error_validate = 1
            error_msg +=" Please enter workflow_name, "
        if "node_details" not in data :
            error_validate = 1
            error_msg +=" Please enter node_details, "

        return {"error": error_validate, "error_msg": error_msg}

    def create_or_update(self, request):
        try:           
            log.info(request.data)
            user_id = request.user.id
            workflow_data = {}
            workflow_data_set = {}
            status_name = ''
            workflow_old_value = {}
            last_status = ''
            table_name = "workflow_master"
            validate = CommonWorkflowMasterController.field_validation(request.data)
            if validate['error'] == 1:
                return { "error_code": True, "message": validate['error_msg'] }

            if "workflow_id" in request.data and request.data['workflow_id']:
                workflow_data_set = WorkflowMaster.objects.get(workflow_id = request.data['workflow_id'])
                print(workflow_data_set)
                if not workflow_data_set:
                    return { "error_code": True, "message": "Record not found please enter valid ID" }
 
            if "workflow_id" in request.data and "workflow_name" in request.data and request.data['workflow_name'] and request.data['workflow_id']:
                query_set = WorkflowMaster.objects.exclude(workflow_id=request.data['workflow_id']).filter(workflow_name__iexact = request.data['workflow_name'])
                if query_set:
                    return {"error_code": True, "message": "Workflow name already exists" }
            else:
                query_set = WorkflowMaster.objects.filter(workflow_name__iexact = request.data['workflow_name'])
                if query_set:
                    return {"error_code": True, "message": "Workflow name already exists" }

            if "workflow_name" in request.data and request.data['workflow_name']:
                    workflow_data['workflow_name'] = request.data['workflow_name']
            else:
                count_off = WorkflowMaster.objects.all().count() 
                workflow_data['workflow_name'] = "workflow_"+str(count_off)       

            if "node_details" in request.data and request.data['node_details']:
                if isinstance(request.data['node_details'], list):
                    node_details =  request.data['node_details']
                else:
                    node_details = json.loads(request.data['node_details'])  
                workflow_data['node_details'] = node_details 
            if "workflow_id" in request.data and request.data['workflow_id']:
                last_status = workflow_data_set.workflow_status
                workflow_old_data = WorkflowMaster.objects.filter(workflow_id = request.data['workflow_id'])
                workflow_old_data_serializer = WorkflowMasterSerializer(workflow_old_data, many= True)
                if workflow_old_data_serializer.data:
                    workflow_old_value = json.loads(json.dumps(workflow_old_data_serializer.data))
                workflow_data['created_by'] = workflow_data_set.created_by
                if "workflow_status" in request.data and request.data['workflow_status']:
                    workflow_data['workflow_status'] = StatusEnum[request.data['workflow_status']].value
                    status_name = StatusEnum[request.data['workflow_status']].name
                else:
                    workflow_data['workflow_status'] = workflow_data_set.workflow_status
                    status_name = StatusEnum(workflow_data_set.workflow_status).name
                if "publish" in request.data and request.data['publish']:
                    workflow_data['workflow_status'] = StatusEnum.Active.value
                    status_name = StatusEnum.Active.name
                datamap_data = WorkflowDataMap.objects.filter(workflow_id=request.data['workflow_id']).values_list('data_map_id', flat=True)
                updated_date = datetime.datetime.now()
                if datamap_data:
                    DataMap.objects.filter(data_map_id__in = datamap_data,is_deleted=False).update(status=workflow_data['workflow_status'],updated_by = request.user.id, updated_date = updated_date)
                    if settings.AIRFLOW_ENABLE == '1':
                        try:    
                            for value_dmi in datamap_data:
                                log.info("Calling Airflow in Workflow creation for data map id {}".format(value_dmi))
                                create_dag(value_dmi)
                        except Exception as e:
                            log.error(e)

                workflow_data['updated_by']= request.user.id
                workflow_data['updated_date']= updated_date
                workflow_save = WorkflowMasterSerializer(workflow_data_set, data = workflow_data)  
            else:          
                workflow_data['created_by'] = request.user.id
                if "workflow_status" in request.data and request.data['workflow_status']:
                    workflow_data['workflow_status'] = StatusEnum[request.data['workflow_status']].value
                    status_name = StatusEnum[request.data['workflow_status']].name
                else:
                    workflow_data['workflow_status'] = StatusEnum.Draft.value
                    status_name = StatusEnum.Draft.name
                if "publish" in request.data and request.data['publish']:
                    workflow_data['workflow_status'] = StatusEnum.Active.value
                    status_name = StatusEnum.Active.name
                workflow_data['updated_date']=datetime.datetime.now()
                workflow_save = WorkflowMasterSerializer(data = workflow_data)   
            
            if workflow_save.is_valid():
                tm_inserted_id = workflow_save.save()
                workflow_data['workflow_id']= tm_inserted_id.workflow_id
                workflow_data['workflow_status']  =  StatusEnum(workflow_data['workflow_status']).name
                if workflow_old_value:
                    self.node_history( workflow_data, user_id, old_data = workflow_old_value[0])
                    workflow_old_value[0]['workflow_status'] = StatusEnum(workflow_old_value[0]['workflow_status']).name
                    wd_values_changed = DeepDiff(workflow_old_value[0],workflow_data, exclude_paths = ["root['workflow_id']", "root['node_details']", "root['is_deleted']", "root['created_by']", "root['created_date']", "root['updated_by']", "root['updated_date']"]  , ignore_order = True, report_repetition=True)
                    self.history_data_format(wd_values_changed, workflow_data, user_id, table_name, workflow_data['workflow_id'], name= workflow_data['workflow_name'])
                else:
                    historyStatus = "Create Workflow"
                    self.history_create(workflow_id = tm_inserted_id.workflow_id, user_id = user_id, table_name = table_name, history_status = historyStatus, description="New Workflow added", changed_attributes=workflow_data)
                    self.node_history( workflow_data, user_id, old_data = '')

                if 'workflow_connection' in request.data and request.data['workflow_connection']:
                    wkc_response = CommonWorkflowMasterController.workflow_connection_create_or_update(tm_inserted_id.workflow_id, request.data['workflow_connection'], request.user.id)
                    if wkc_response['error_code']:
                        res_data ={"workflow_id" : tm_inserted_id.workflow_id, "updated_date":tm_inserted_id.updated_date, "status_name": status_name, 'workflow_name': tm_inserted_id.workflow_name}
                        return {"error_code": False, "message": wkc_response['message'], "data":res_data}  

                if 'workflow_datamap' in request.data and request.data['workflow_datamap']:
                    workflowDataMapResponse = CommonWorkflowMasterController.workflow_datamap_create_or_update(tm_inserted_id.workflow_id, request.data['workflow_datamap'], request.user.id)
                    if workflowDataMapResponse['error_code']:
                        res_data ={"workflow_id" : tm_inserted_id.workflow_id, "updated_date":tm_inserted_id.updated_date, "status_name": status_name, 'workflow_name': tm_inserted_id.workflow_name}
                        return {"error_code": False, "message": workflowDataMapResponse['message'], "data":res_data}  

                res_data ={"workflow_id" : tm_inserted_id.workflow_id, "updated_date":tm_inserted_id.updated_date, "status_name": status_name, 'workflow_name': tm_inserted_id.workflow_name}

                if ("publish" in request.data and request.data['publish']) and (last_status ==3):
                    trigger_response = CommonWorkflowMasterController.trigger_airflow(self,request,triggered_from="workflow_publish")                
                    res_data['airflow_response'] = trigger_response['message']

                return {"error_code": False, "message":"Success", "data":res_data}              
            else:
                return { "error_code": True, "message": str(workflow_save.errors) }   
        except Exception as e:
            log.error(e)
            return {"error_code": True, "message": str(e) }
        except KeyError as err:
            log.error(err)
            return {"error_code": True, "message": str(err) }

    def workflow_connection_create_or_update(self, workflow_id, workflow_connection, user_id): 
        try:           
            log.info(workflow_connection)
            workflow_data = {}  
            if workflow_id:
                if workflow_connection:
                    if isinstance(workflow_connection, list):
                        workflow_connection_conv =  workflow_connection
                    else:
                        workflow_connection_conv = json.loads(workflow_connection)
                else:
                    workflow_connection_conv = []


                if workflow_connection_conv:

                    # dataMapCount = WorkflowConnection.objects.filter(workflow_id = workflow_id).count()
                    # print(dataMapCount)
                    # print(type(dataMapCount))
                    # if(dataMapCount > 0):
                    #     WorkflowConnection.objects.filter(workflow_id = workflow_id).delete()
                    #     print("WorkflowDataMap found")  

                    wkc_error_validate = 2
                    wkc_error_msg=[]
                    updated_date = datetime.datetime.now()
                    wc_old_value = {}
                    table_name = "workflow_connection"
                    for wck_data in workflow_connection_conv:
                        print("b if")
                        if "connection_id" not in wck_data or not wck_data['connection_id']:
                            wkc_error_validate = 1
                            wkc_error_msg.append("connection Id not found")
                            continue
                        dataMapCount = WorkflowConnection.objects.filter(workflow_id = workflow_id, connection_node_id = wck_data['connection_node_id'])
                        print(dataMapCount)
                        print(type(dataMapCount))
                        dc_serializer = WorkflowConnectionSerializer(dataMapCount, many=True)
                        if dc_serializer.data:
                            wc_old_value = json.loads(json.dumps(dc_serializer.data))

                        if(dataMapCount):
                            dataMapCount = WorkflowConnection.objects.get(workflow_id = workflow_id, connection_node_id = wck_data['connection_node_id'])
                            print("WorkflowConnection found")
                            wck_data['created_by'] = dataMapCount.created_by
                            wck_data['updated_by']= user_id
                            wck_data['updated_date']= updated_date
                            ConnSerializer = WorkflowConnectionSerializer(dataMapCount, data = wck_data)
                            # Check Workflow Id is exsist or not.
                        else:    
                            wck_data['created_by'] = user_id
                            wck_data['updated_date']= updated_date
                            ConnSerializer = WorkflowConnectionSerializer(data = wck_data)
                        if(ConnSerializer.is_valid()):
                            wc_inserted_id = ConnSerializer.save()

                            connection_data = Connection.objects.get(connection_id= wck_data['connection_id'])

                            if wc_old_value:
                                wd_values_changed = DeepDiff(wc_old_value[0],wck_data, exclude_paths = ["root['workflow_data_map_id']" , "root['data_map_name']", "root['workflow_id']", "root['is_deleted']", "root['created_by']", "root['created_date']", "root['updated_by']", "root['updated_date']", "root['field_backend_name']" ], ignore_order = True, report_repetition=True)
                                self.history_data_format(wd_values_changed, wck_data, user_id, table_name, workflow_id, name = connection_data.name + " connection")
                            else:
                                self.history_data_format_create(wck_data, user_id, table_name, workflow_id) 
                        else:
                            wkc_error_validate = 1
                            wkc_error_msg.append(ConnSerializer.errors)
                        print("connection End")
                    if wkc_error_validate == 2:
                        return {"error_code": False, "message": "Data Added in Workflow connection", "data": {"updated_date": updated_date}}
                    else:
                        return {"error_code": True, "message": wkc_error_msg }      
                    # return {"error_code": False, "message": "workflow created success" }
                else:
                    return {"error_code": False, "message": "workflow connection details not found" }    

            else:
                return {"error_code": True, "message": "workflow id not found" }        
        except Exception as e:
            log.error(e)
            return {"error_code": True, "message": str(e) }
        except KeyError as err:
            log.error(err)
            return {"error_code": True, "message": str(err) }         

   

    def workflow_datamap_create_or_update(workflow_id, requestData, user_id):
        try:            
            if isinstance(requestData, list):
                if len(requestData) > 0:
                    wkc_error_validate = 2
                    wkc_error_msg=[]
                    updated_date = ''
                    dataMapCount = WorkflowDataMap.objects.filter(workflow_id = workflow_id).count()
                    if(dataMapCount > 0):
                        WorkflowDataMap.objects.filter(workflow_id = workflow_id).delete()
                        print("WorkflowDataMap found")
                    for data in requestData:                       
                        data['created_by'] = user_id
                        data['updated_date']=datetime.datetime.now()
                        dataMapSerializer = WorkflowDataMapSerializer(data = data)
                        if(dataMapSerializer.is_valid()):
                            dataMapSerializer.save()
                        else:
                            wkc_error_validate = user_id
                            wkc_error_msg.append(dataMapSerializer.errors)
                        updated_date = data['updated_date']
                    if wkc_error_validate == 2:
                        return {"error_code": False, "message": "Data Added in Workflow Datamap", "data": {"updated_date": updated_date}}
                    else:
                        return {"error_code": True, "message": wkc_error_msg }        
                else:
                    return {"error_code": False, "message": "workflow Datamap details not found" }        
            else:   
                 raise KeyError
        except KeyError:
            log.error("Invalid Request Fields")
            return {"error_code": True, "message": "Invalid Request Fields" }     

        except Exception as e:
            log.error(e)
            return {"error_code": True, "message": str(e) }


    def workflow_datamap_new_create_or_update(self, requestData, user_id):
        try:
            print("inside new ")
            if isinstance(requestData, dict):
                if len(requestData) > 0:
                    wkc_error_validate = 2
                    wkc_error_msg=[]
                    updated_date = ''
                    wd_old_value = {}
                    wd_values_changed = {}
                    table_name = "workflow_datamap"
                    wd_data = {}
                    print("b if")
                    # print(requestData)
                    if "data_map_id" not in requestData or not requestData['data_map_id']:
                        wkc_error_validate = 1
                        wkc_error_msg.append("Data map Id not found")
                    dataMapCount = WorkflowDataMap.objects.filter(workflow_id = requestData['workflow_id'], source_connection_node_id = requestData['source_connection_node_id'], destination_connection_node_id = requestData['destination_connection_node_id'])
                    dm_serializer = WorkflowDataMapSerializer(dataMapCount, many = True)
                    if dm_serializer.data:
                        wd_old_value = json.loads(json.dumps(dm_serializer.data))

                    if(dataMapCount):
                        dataMapCount = WorkflowDataMap.objects.get(workflow_id = requestData['workflow_id'], source_connection_node_id = requestData['source_connection_node_id'], destination_connection_node_id = requestData['destination_connection_node_id'])
                        print("DatamapConnection found")
                        if dataMapCount.data_map_id != requestData['data_map_id']:
                            requestData['is_deleted'] = False
                        requestData['created_by'] = dataMapCount.created_by
                        requestData['updated_by']= user_id
                        requestData['updated_date']=datetime.datetime.now()
                        DataSerializer = WorkflowDataMapSerializer(dataMapCount, data = requestData)
                        # Check Workflow Id is exsist or not.
                    else:
                        requestData['created_by'] = user_id
                        requestData['updated_date']=datetime.datetime.now()
                        DataSerializer = WorkflowDataMapSerializer(data = requestData)
                    updated_date = requestData['updated_date']
                    if(DataSerializer.is_valid()):
                        DataSerializer.save()
                        # Get connection_name
                        con_condition = [requestData['source_connection_node_id'],requestData['destination_connection_node_id']]
                        connection_data = WorkflowConnection.objects.filter(connection_node_id__in =con_condition )                        
                        connection_data_serializer = WorkflowConnectionListDetailsSerializer(connection_data, many=True)
                        # Get data_map_name
                        data_map_name = DataSerializer.data['data_map_name']
                        if wd_old_value:
                            wd_values_changed = DeepDiff(wd_old_value[0],requestData, exclude_paths = ["root['workflow_data_map_id']" , "root['data_map_name']", "root['workflow_id']", "root['is_deleted']", "root['created_by']", "root['created_date']", "root['updated_by']", "root['updated_date']"], ignore_order = True, report_repetition=True)
                            self.history_data_format(wd_values_changed, requestData, user_id, table_name, requestData['workflow_id'], name= data_map_name + " Datamap")
                        else:
                            historyStatus = "Create Workflow"
                            self.history_data_format_create(requestData, user_id, table_name, requestData['workflow_id'])
                            self.history_create(requestData['workflow_id'], user_id, table_name, historyStatus, description=data_map_name + " DataMap Established between "+ connection_data_serializer.data[0]['connection_name'] + " & " + connection_data_serializer.data[1]['connection_name'],  changed_attributes =requestData)
                    else:
                        wkc_error_validate = 1
                        wkc_error_msg.append(DataSerializer.errors)
                    if wkc_error_validate == 2:
                        return {"error_code": False, "message": "Data Added in Workflow Datamap", "data": {"updated_date": updated_date} }
                    else:
                        return {"error_code": True, "message": wkc_error_msg }
                else:
                    return {"error_code": False, "message": "workflow Datamap details not found" }
            else:
                 raise KeyError
        except KeyError:
            log.error("Invalid Request Fields")
            return {"error_code": True, "message": "Invalid Request Fields" }

        except Exception as e:
            log.error(e)
            return {"error_code": True, "message": str(e) }

    def history_create(self, workflow_id, user_id, table_name, history_status, description, changed_attributes = '' ):
        try:
            wd_history_data = {}
            if description and history_status and table_name:
                wd_history_data['workflow_id'] = workflow_id
                wd_history_data['changed_attributes'] =  ''
                if changed_attributes:
                    wd_history_data['changed_attributes'] = json.dumps(str(changed_attributes))
                wd_history_data['table_name'] = table_name
                wd_history_data['history_status'] = history_status
                wd_history_data['description'] = description
                wd_history_data['created_by'] = user_id
                wd_history_data['created_date'] = datetime.datetime.now()
                wd_history_save = WorkflowHistorySerializer(data = wd_history_data)
                if wd_history_save.is_valid():
                    wh_inserted_id = wd_history_save.save()
                else:
                    log.info(wd_history_save.errors)

            else:
                 log.info("No data found in history")

        except Exception as e:
            print(str(e))
            return ''

    def history_data_format(self, wd_values_changed, requestData, user_id, table_name, workflow_id, name):
        try:
            flag = ''
            workflow_data = WorkflowMaster.objects.get(workflow_id=workflow_id)
            for key in wd_values_changed:
                history_status = ''
                if "iterable_item_removed" == key:
                    if workflow_data.workflow_status != 3:
                        history_status = name + " was modified"
                    else:
                        history_status = "Create Workflow"
                    for mapping in wd_values_changed[key]: 
                        msgs = ''
                        design = wd_values_changed[key][mapping].items()
                        for key3, value3 in design:
                            if value3:
                                msgs += key3+ " is " + value3
                                msgs = msgs + ", "
                        if msgs:
                            description = "Value removed from {}".format(msgs)
                            description = description[:-2]
                            self.history_create(workflow_id,user_id, table_name, history_status, description,  changed_attributes =wd_values_changed)
                elif "iterable_item_added" == key:
                    if workflow_data.workflow_status != 3:
                        history_status = name + " was modified"
                    else:
                        history_status = "Create Workflow"
                    for mapping in wd_values_changed[key]: 
                        msg = ''
                        design = wd_values_changed[key][mapping].items()
                        for key1, value1 in design:
                            if value1:
                                msg += key1+ " is " + value1
                                msg = msg + ", "
                        if msg:
                            description = "Values added to {}".format(msg)
                            description = description[:-2]
                            self.history_create(workflow_id, user_id, table_name, history_status, description ,  changed_attributes =wd_values_changed)
                elif "values_changed" == key:
                    if workflow_data.workflow_status != 3:
                        history_status = name + " was modified"
                    else:
                        history_status = "Create Workflow"
                    if table_name == "workflow_master":
                        history_status = "Workflow Edit"
                    message = ''
                    column_name = wd_values_changed[key]
                    for key2 in column_name:
                        spl = key2.split('[')[-1].split(']')[0]
                        ml_model = ("'algorithm_id'", "'description'", "'ml_model_id'")
                        if spl not in ml_model:
                            message += spl + " has changed from " + str(column_name[key2]['old_value']) + " to " +  str(column_name[key2]['new_value'])
                            if list(column_name.keys())[-1] != key2:
                                message = message + ", "
                        # message += spl + " has changed from " + str(column_name[key2]['old_value']) + " to " +  str(column_name[key2]['new_value'])
                        # if list(column_name.keys())[-1] != key2:
                        #     message = message + ", "
                        if "workflow_status" in spl and str(column_name[key2]['old_value']) == "Draft" and str(column_name[key2]['new_value']) == "Active":
                            flag = "published"
                            history_status = "Workflow Published"
                            message = str(workflow_id) + " - Workflow is in Active state"
                        if "data_map_id" in spl:
                            message = ''
                            datamap_details = DataMap.objects.get(data_map_id = requestData['data_map_id'])
                            datamap_serializer = DataMapSerializer(datamap_details)
                            description = datamap_serializer.data['data_map_name'] + " DataMap Established between " + datamap_serializer.data['source_connection_name'] + " & " + datamap_serializer.data['destination_connection_name']
                            self.history_create(workflow_id, user_id, table_name, history_status="Workflow Modified", description = description,  changed_attributes =wd_values_changed)
                    if flag == "published":
                        description = message
                    else:
                        description = "Values changed in {}".format(message)
                    if message:
                        self.history_create(workflow_id, user_id, table_name, history_status, description,  changed_attributes =wd_values_changed)
            
        except Exception as e:
            print(str(e))
            return ''


    def history_data_format_create(self, requestData, user_id, table_name, workflow_id):
        try:
            description = ''
            if table_name == "workflow_datamap":
                if "data_field_mapping" in requestData['properties']:
                    history_status = "Create Workflow"
                    field_mapping = requestData['properties']['data_field_mapping']
                    for key1 in field_mapping:
                        msg = ''
                        for key2 in key1:
                            msg += key2+ " is " + str(key1[key2])
                            if list(key1.keys())[-1] != key2:
                                msg = msg + ", "
                        description = "Values added to {}".format(msg)
                        # description = description[:-2]
                        self.history_create(workflow_id,user_id, table_name, history_status, description,  changed_attributes =requestData)
                      
            elif table_name == "workflow_connection":
                history_status = "Create Workflow"            
                if "source_actual_file_name" in requestData['rules'] and requestData['rules']['source_actual_file_name']:
                    description = "File "+ str(requestData['rules']['source_actual_file_name']) + " was uploaded"
                    self.history_create(workflow_id,user_id, table_name, history_status, description,  changed_attributes =requestData)
                if "source_header_list" in requestData['rules'] and requestData['rules']['source_header_list']:
                    header_list = ", ".join(requestData['rules']['source_header_list'])
                    description = "File header added '" + header_list + "'"
                    self.history_create(workflow_id,user_id, table_name, history_status, description,  changed_attributes =requestData)
                if  "source_api_names" in requestData['rules'] and requestData['rules']['source_api_names']:
                    description = "Values added to source_api_names is " + str(requestData['rules']['source_api_names'])
                    self.history_create(workflow_id,user_id, table_name, history_status, description,  changed_attributes =requestData)
                if "schedule_json" in requestData['rules'] and requestData['rules']['schedule_json']:
                    if "schedule" in requestData['rules']['schedule_json'][0] and requestData['rules']['schedule_json'][0]['schedule']:
                        for schedule in requestData['rules']['schedule_json']:
                            msg = ''
                            for key2 in schedule:
                                if schedule[key2]:
                                    msg += key2+ " is " + str(schedule[key2])
                                    if list(schedule.keys())[-1] != key2:
                                        msg = msg + ", "
                            description = "Values added to {}".format(msg)  
                            self.history_create(workflow_id,user_id, table_name, history_status, description,  changed_attributes =requestData)
        except Exception as e:
            print(str(e))
            return ''

    def node_history(self, workflow_data, user_id, old_data=''):
        try:
            connection_name = ''
            if old_data:
                if old_data['node_details']['nodes'] and workflow_data['node_details']['nodes']:
                    node_key = "connName"
                    old_node_details = [a_dict[node_key] for a_dict in old_data['node_details']['nodes']]
                    new_node_details = [a_dict[node_key] for a_dict in workflow_data['node_details']['nodes']]
                    set_difference = set(new_node_details) - set(old_node_details)
                    if len(old_node_details)==1 and len(new_node_details) ==1 :
                        c_name = str(new_node_details)[2:-2]             
                        history_data = WorkflowHistory.objects.filter(description__icontains = c_name, workflow_id = workflow_data['workflow_id'])
                        if not history_data:
                            connection_name = str(new_node_details)[2:-2]
                    if set_difference and len(old_node_details) != len(new_node_details):
                        connection_name = str(set_difference)[2:-2]
            else:
                if workflow_data['node_details'] and workflow_data['node_details']['nodes'][0]:
                    if "connName" in workflow_data['node_details']['nodes'][0] and workflow_data['node_details']['nodes'][0]['connName']:
                        connection_name = workflow_data['node_details']['nodes'][0]['connName']
            if connection_name:
                description =  connection_name + " connection added"
                self.history_create(workflow_data['workflow_id'],user_id, "workflow_connection", "Create Workflow", description,  changed_attributes =workflow_data)
        except Exception as e:
            print(str(e))
            return ''

    def create_or_update_algorithm_detail(self, request):
        try:
            log.info(request.data)
            algorithm_detail_data = {}
            algorithm_detail_set = {}
            data_map_id = 0

            if request.data:
                if "algorithm_detail_id" in request.data and request.data['workflow_id']:
                    algorithm_detail_set = MLAlgorithmDetail.objects.get(algorithm_detail_id = request.data['algorithm_detail_id'])
                    if not algorithm_detail_set:
                        return { "error_code": True, "message": "Record not found please enter valid ID" }

                if "workflow_id" in request.data and request.data["workflow_id"]:
                    algorithm_detail_data['workflow_id'] = request.data['workflow_id']
                else:
                    return {"error_code": True, "message": "Please provide workflow_id" }

                if "data_map_id" in request.data and request.data["data_map_id"]:
                    algorithm_detail_data['data_map_id'] = request.data['data_map_id']
                    data_map_id = request.data['data_map_id']
                else:
                    return {"error_code": True, "message": "Please provide data_map_id" }

                if "algorithm_id" in request.data and request.data["algorithm_id"]:
                    algorithm_detail_data['algorithm_id'] = request.data['algorithm_id']
                else:
                    return {"error_code": True, "message": "Please provide algorithm_id" }

                if "file_id" in request.data and request.data["file_id"]:
                    algorithm_detail_data['file_id'] = request.data['file_id']
                else:
                    return {"error_code": True, "message": "Please provide file_id" }

                if "is_training" in request.data and request.data["is_training"]:
                    algorithm_detail_data['is_training'] = request.data['is_training']
                else:
                    algorithm_detail_data['is_training'] = StatusEnum.Inactive.value

                if "label" in request.data and request.data['label']:
                    algorithm_detail_data['label'] = request.data['label']

                if "extras" in request.data and request.data['extras']:
                    algorithm_detail_data['extras'] = request.data['extras']
    
                if "algorithm_detail_id" in request.data and request.data['workflow_id']:
                    algorithm_detail_data['status'] = algorithm_detail_set.status
                    if "status" in request.data and request.data["status"]:
                        algorithm_detail_data['status'] = request.data['status']
                    algorithm_detail_data['updated_by'] = request.user.id
                    algorithm_detail_data['updated_date'] = datetime.datetime.now()
                    algorithm_detail_save = MLAlgorithmDetailUpdateSerializer(algorithm_detail_set, data = algorithm_detail_data)
                else:
                    algorithm_detail_data['status'] = StatusEnum.Active.value
                    algorithm_detail_data['created_by'] = request.user.id
                    algorithm_detail_data['created_date'] = datetime.datetime.now()
                    algorithm_detail_save = MLAlgorithmDetailCreateSerializer(data = algorithm_detail_data)

                if algorithm_detail_save.is_valid():
                    inserted_id = algorithm_detail_save.save()
                    if settings.AIRFLOW_ENABLE == '1':
                        try:
                            # calling airflow
                            log.info("Airflow call to update ML Algorithm for workflow_id - {} data_map_id - {}".format(request.data["workflow_id"],data_map_id))
                            create_dag_ml(data_map_id)
                        except Exception as e:
                            log.error(e)
                    if "algorithm_detail_id" in request.data and request.data["algorithm_detail_id"]:
                        success_message = "Algorith Detail id - " + str(request.data["algorithm_detail_id"]) + " updated successfully"
                    else:
                        success_message = "Algorith Detail id -" + str(inserted_id.algorithm_detail_id) + " created successfully"

                    return {"error_code": False, "message": success_message }
                else:
                    log.info(algorithm_detail_save.errors)
                    return {"error_code": True, "message": "Error in database" }
            else:
                return {"error_code": True, "message": "No input" }
        except Exception as e:
        # Application failure content
            return Response({"error": True, "message": e, "status": 400}, status=status.HTTP_400_BAD_REQUEST)

    def trigger_airflow(self,request,triggered_from):
        try:
            log.info("Trigger workflow")
            condition = Q(is_deleted= False)
            condition.add(Q(source_connection_id__in = Connection.objects.filter(connection_type_id = 12).values_list('connection_id', flat=True)), Q.AND)
            condition.add(Q(data_map_id__in = WorkflowDataMap.objects.filter(workflow_id=request.data['workflow_id']).values_list('data_map_id', flat=True)), Q.AND)
            manual_data_map = DataMap.objects.filter(condition).values_list('data_map_id', flat=True)
            
            if settings.AIRFLOW_ENABLE == '1' and manual_data_map:
                try:
                    for value_dmi in manual_data_map:
                        log.info("Triggering Airflow in Workflow when Workflow is published for data map id {}".format(value_dmi))
                        airflow_trigger_dag = {}
                        trigger_count =1
                        dag_response_new = {}
                        aiflow_response = {}
                        dag_response = trigger_workflow(value_dmi)
                        aiflow_response['first_response'] = str(dag_response)
                        if dag_response['status_code'] != 200 and triggered_from == "workflow_publish":
                            dag_response_new = trigger_workflow(value_dmi)
                            aiflow_response['second_response'] = str(dag_response_new)
                            trigger_count +=1

                        airflow_trigger_dag['workflow_id'] = request.data['workflow_id']
                        airflow_trigger_dag['data_map_id'] = value_dmi
                        airflow_trigger_dag['dag_response_code'] = dag_response['status_code']
                        airflow_trigger_dag['dag_response'] = str(aiflow_response)
                        airflow_trigger_dag['triggered_from'] = triggered_from
                        airflow_trigger_dag['trigger_count'] = trigger_count
                        airflow_trigger_dag['created_by'] = request.user.id
                        airflow_trigger_dag['status'] = StatusEnum.Active.value
                        airflow_trigger_dag_save = AirflowTriggerDagSerializer(data =airflow_trigger_dag )
                        if airflow_trigger_dag_save.is_valid():
                            ad_inserted_id = airflow_trigger_dag_save.save()
                            log.info("airflow_trigger_dag_id :  {}".format(ad_inserted_id))

                    if dag_response['status_code'] == 200:
                        return {"error_code": False, "message": "Airflow Triggered Successfully" }
                    else:
                        return {"error_code": True, "message": "Airflow is in running stage please try again after few minutes" }
                except Exception as e:
                    log.error(e)
                    return {"error_code": True, "message": str(e) }
            else:
                return {"error_code": True, "message": "Manual file upload connection not found" }
        except Exception as err:
            log.error(err)
            return {"error_code": True, "message": str(err) }
