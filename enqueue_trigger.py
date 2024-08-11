from sqlalchemy.exc import IntegrityError
from airflow.models import Variable
from airflow.api.common.experimental import trigger_dag



def trigger_enqueue_with_microseconds(conf):     
    var_name = "USE_RABBIT_MQ"
    use_rabbit_mq = Variable.get(var_name, default_var = "YES") 
    not_triggered = True
    while not_triggered:
        try:
            if use_rabbit_mq == "YES":
                trigger_dag.trigger_dag(dag_id='ENQUEUE_TASKS', conf=conf, replace_microseconds = False)
            else:
                trigger_dag.trigger_dag(dag_id = 'ENQUEUE_TO_SERVICE_BUS', conf = conf, replace_microseconds = False)
            not_triggered = False
        except IntegrityError:
            continue


def trigger_etl_task_without_error(conf):
    not_triggered = True
    while not_triggered:
        try:
            trigger_dag.trigger_dag(dag_id = "TEXT_ANALYTICS_ETL", conf = conf, replace_microseconds = False)
            not_triggered = False
        except IntegrityError:
            continue