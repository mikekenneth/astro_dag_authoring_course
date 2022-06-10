from airflow.models import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context


@task.python
def process_a():
    # Pull Parthenr info from XCOM
    ti = get_current_context()["ti"]
    # In this case we had to add the dag_id becauce the XCOM is in antoher DAG (in this case parent DAG)
    partner_name = ti.xcom_pull(key="partner_name", task_ids="extract_partners", dag_id="my_dag")
    partner_path = ti.xcom_pull(key="partner_path", task_ids="extract_partners", dag_id="my_dag")
    print(partner_name)
    print(partner_path)


@task.python
def process_b():
    # Pull Parthenr info from XCOM
    ti = get_current_context()["ti"]
    # In this case we had to add the dag_id becauce the XCOM is in antoher DAG (in this case parent DAG)
    partner_name = ti.xcom_pull(key="partner_name", task_ids="extract_partners", dag_id="my_dag")
    partner_path = ti.xcom_pull(key="partner_path", task_ids="extract_partners", dag_id="my_dag")
    print(partner_name)
    print(partner_path)


@task.python
def process_c():
    # Pull Parthenr info from XCOM
    ti = get_current_context()["ti"]
    # In this case we had to add the dag_id becauce the XCOM is in antoher DAG (in this case parent DAG)
    partner_name = ti.xcom_pull(key="partner_name", task_ids="extract_partners", dag_id="my_dag")
    partner_path = ti.xcom_pull(key="partner_path", task_ids="extract_partners", dag_id="my_dag")
    print(partner_name)
    print(partner_path)


def subdag_factory(parent_dag_id, subdag_dag_id, defaults_args):
    with DAG(f"{parent_dag_id}.{subdag_dag_id}", default_args=defaults_args) as dag:
        process_a()
        process_b()
        process_c()
    return dag
