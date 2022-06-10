from airflow.decorators import task, dag
from airflow.operators.subdag import SubDagOperator

from datetime import datetime, timedelta

from subdag.subdag_factory import subdag_factory


@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}


default_args = {
    "start_date": datetime(2022, 6, 9),
}


@dag(
    description="DAG for Learning Authoring",
    default_args=default_args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1,
)
def my_dag():
    partner_settings = extract()

    process_tasks = SubDagOperator(
        task_id="process_tasks", subdag=subdag_factory("my_dag", "process_tasks", default_args, partner_settings)
    )

    extract() >> process_tasks


dag = my_dag()
