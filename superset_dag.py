from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from helper.utils import _cluster_logging
from superset.api import SuperSetApi

dag_tags = ['opexwise', "superset"]
default_args = {
    'owner': 'Parthiban.S',
    'depends_on_past': False,
    'start_date': datetime(2021, 8, 18),
    'email': ['parthiban.sivasamy@iopex.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'concurrency': 1,
    'max_active_runs': 1,
    'catchup_by_default': False
}

def create_superset_dataset(**kwargs):
        """
        Creating dataset in SuperSet
        """
        print(kwargs)
        table_name = kwargs["params"]["table_name"]
        cluster_job_ids = kwargs['params']['cluster_job_ids']
        reference_id = kwargs['params']["reference_id"]
        logging = _cluster_logging(kwargs, cluster_job_ids)
        key = "DS_TABLE_{}".format(str(table_name))
        ds_created_in_superset = Variable.get(key, default_var=None)
        if ds_created_in_superset is None:
            logging.info("Making SuperSet dataset for the table : {}".format(table_name))
            try:
                response = SuperSetApi().set_config().create_dataset(table_name)
                if response['status']:
                    Variable.set(key, "YES")
                    if response['status_code'] == 201:
                        logging.info("Successfully created DataSet {0} in SuperSet".format(str(table_name)))
                        logging.update_process_status(
                            job_status=8,
                            reference_id=reference_id
                            )
                    if response['status_code'] == 422:
                        logging.debug("Dataset {0} already exists".format(str(table_name)))
                else:
                    logging.debug("DataSet not created in SuperSet. Please check the configuration.")
            except Exception as e:
                print("Exception  : {0}".format(str(e)))
                logging.error("DataSet not created in SuperSet. Please contact support.")
        else:
            logging.update_process_status(
                                job_status=8,
                                reference_id=reference_id
                                )

with DAG(dag_id='SUPER_SET_ANALYSIS',
         default_args=default_args,
         schedule_interval=None,
         tags=['superset'],
         max_active_runs=1,
         catchup=False) as dag:

    dag.doc_md = """
            #### Superset
            Triggered by dequeue dag
            """

    task_super_set = PythonOperator(
        task_id="Super_Set",
        provide_context=True,
        python_callable=create_superset_dataset,
    )
