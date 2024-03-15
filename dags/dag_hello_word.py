try:
    import sys
    import os
    sys.path.insert(0, os.path.abspath(os.curdir))
except ModuleNotFoundError:
    pass
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator


def somar(a, b):
    print(a+b)


@dag(schedule_interval=None, start_date=datetime(2024, 3, 14), catchup=False, tags=['example'])
def custom_tg():
    @task
    def get_num_1():
        print('numero 1')

    lista_lista = [(1, 2), (3, 4)]

    @task_group(group_id='task_soma')
    def task_soma():
        lista_task = []
        for c, l in enumerate(lista_lista):

            task_soma = PythonOperator(
                task_id=f'somar_{c}',
                python_callable=somar,
                op_kwargs={'a': [0], 'b': [1]}
            )
            lista_task.append(task_soma)

    get_num_1() >> task_soma()


custom_tg()
