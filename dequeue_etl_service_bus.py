from datetime import datetime, timedelta

from airflow.models import DAG, XCom
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from helper.get_execution_date import get_most_recent_dag_run
from airflow.utils.timezone import make_aware
from operators.service_bus_pull import ServiceBusPullOperator
from operators.redis_task_queue import RedisTaskQueueOperator

from destination.destinations import batch_message_etl

dag_tags = ['opexwise', "etl"]

ETL_QUEUE = Variable.get("ETL_QUEUE_NAME", default_var="ETL_DEV")

default_args = {
    'owner': 'Sudarshan.Selvamani',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 18),
    'email': ['sudarshan.selvamani@iopex.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
    'max_active_runs': 1,
    'catchup_by_default': False
}

doc_md = """
            #### ETL Process
            Gets all the messages from service bus and send it to text analytics
            """


def choose_destination(**kwargs):
    var_name = "messages"+ kwargs["run_id"] + kwargs["dag"].dag_id
    input_data = Variable.get(var_name, default_var = None)
    print("check input data",input_data)
    if input_data is not None and input_data != '':
        return ['ETL_TASK', 'CLEAR_XCOM']
    else:
        return 'END'

with DAG(dag_id='ETL_DEQUEUE_SERVICE_BUS_TASKS',
        default_args=default_args,
        schedule_interval= "*/2 * * * *",
        tags=dag_tags, catchup=False) as dag:

    DEQUEUE_ETL_TASK = ServiceBusPullOperator(
        task_id = "SERVICE_BUS_PULL",
        channel=ETL_QUEUE,
        
    )
    
    CHOOSE_DESTINATION = BranchPythonOperator(
        task_id="CHOOSE_DESTINATION",
        provide_context=True,
        python_callable=choose_destination,
    )

    ETL_TASK = PythonOperator(
            task_id="ETL_TASK",
            provide_context=True,
            python_callable = batch_message_etl,
        )
    
    CLEAR_XCOM_TASK = PostgresOperator(
        task_id='CLEAR_XCOM',
        postgres_conn_id='airflow_db',
        sql="delete from xcom where dag_id='{{dag.dag_id}}' and execution_date='{{ts}}'",
        autocommit=True,
        trigger_rule='one_success'
    )

    #sometimes xcom won't be deleted in first try


    RETRY_CLEAR_XCOM_TASK = PostgresOperator(
        task_id='RETRY_CLEAR_XCOM',
        postgres_conn_id='airflow_db',
        sql="delete from xcom where dag_id='{{dag.dag_id}}' and execution_date='{{ts}}'",
        autocommit=True,
        trigger_rule='one_success'
    )

    END = DummyOperator(task_id='END')

    DEQUEUE_ETL_TASK >> CHOOSE_DESTINATION >> \
        [ETL_TASK, END] >> CLEAR_XCOM_TASK >> RETRY_CLEAR_XCOM_TASK
