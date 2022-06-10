from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


@task.python
def process_a(partner_name, partner_path):
    print(partner_name)
    print(partner_path)


@task.python
def process_b(partner_name, partner_path):
    print(partner_name)
    print(partner_path)


@task.python
def process_c(partner_name, partner_path):
    print(partner_name)
    print(partner_path)


@task.python
def check_a():
    print("Checking")


@task.python
def check_b():
    print("Checking")


@task.python
def check_c():
    print("Checking")


def process_tasks(partner_settings):
    with TaskGroup(group_id="process_tasks", add_suffix_on_collision=True) as process_tasks:
        with TaskGroup(group_id="check_tasks") as check_tasks:
            check_a()
            check_b()
            check_c()
        process_a(partner_settings["partner_name"], partner_settings["partner_path"]) >> check_tasks
        process_b(partner_settings["partner_name"], partner_settings["partner_path"]) >> check_tasks
        process_c(partner_settings["partner_name"], partner_settings["partner_path"]) >> check_tasks
    return process_tasks
