from airflow.decorators import task, dag, task_group
from datetime import datetime


@task
def add_task(x, y):
    print(f"Task args: x={x}, y={y}")
    return x + y


@dag(start_date=datetime(2022, 1, 1))
def mydag():
    @task
    def inicio_dag():
        print('ID')

    @task
    def fim_dag():
        print('FD')

    @task_group
    def teste_task_group():

        for i in range(3):
            add_task.override(task_id=f"add_start_{i}")(i, i)

    inicio_dag() >> teste_task_group() >> fim_dag()


mydag()
