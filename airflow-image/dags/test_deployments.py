"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airkupofrod.operator import KubernetesPodOperatorFromDeployment

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("test_deployments", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = DummyOperator(dag=dag, task_id="dummy")

no_deployment = KubernetesPodOperatorFromDeployment(task_id="minimal", dag=dag, image="busybox",
                                                    cmds=["echo", "'hello world'"], in_cluster=True, namespace="default",
                                                    deployment_labels={"app": "minimal"})

t1 >> [
    no_deployment
]