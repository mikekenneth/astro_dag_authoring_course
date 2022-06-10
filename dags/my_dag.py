from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime, timedelta

from groups.process_tasks import process_tasks

partners = {
    "partner_snowflake": {"name": "snowflake", "path": "/partners/snowflake"},
    "partner_netflix": {"name": "netflix", "path": "/partners/netflix"},
    "partner_astronomer": {"name": "astronomer", "path": "/partners/astronomer"},
}


def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    if day == 1:
        return "extract_partner_snowflake"
    if day == 3:
        return "extract_partner_netflix"
    if day == 5:
        return "extract_partner_astronomer"
    return "stop"


@dag(
    description="DAG for Learning Authoring",
    start_date=datetime(2022, 6, 9),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1,
)
def my_dag():
    start = DummyOperator(task_id="start")
    choosing_partner_based_on_day = BranchPythonOperator(
        task_id="choosing_partner_based_on_day", python_callable=_choosing_partner_based_on_day
    )
    stop = DummyOperator(task_id="stop")
    storing = DummyOperator(task_id="storing", trigger_rule="none_failed_or_skipped")

    choosing_partner_based_on_day >> stop
    for partner, details in partners.items():

        @task.python(task_id=f"extract_{partner}", do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name": partner_name, "partner_path": partner_path}

        extracted_values = extract(details["name"], details["path"])
        start >> choosing_partner_based_on_day >> extracted_values
        process_tasks(extracted_values) >> storing


dag = my_dag()
