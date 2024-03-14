try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from airflow.decorators import dag, task, task_group
from pendulum import datetime


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False
)
def dag_hello_word():
    @task
    def exibir_oi():
        print('OI')

    @task
    def exibir_oi_dois():
        print('OI')

    @task
    def exibir_oi_tres():
        print('OI')

    @task_group
    def exibir_varios_oi():
        for _ in range(1, 4):
            exibir_oi_dois()
        exibir_oi_tres()

    exibir_oi() >> exibir_varios_oi()


dag = dag_hello_word()
