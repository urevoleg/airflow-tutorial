import os
import datetime as dt

import requests
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator

args = {
    'owner': 'admin',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False
}

FILENAME = os.path.join(os.path.expanduser("~"), "loads", 'titanic.csv')


def download():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(FILENAME, 'w', encoding='utf-8') as f:
        for chunk in response.iter_lines():
            f.write('{}\\n'.format(chunk.decode('utf-8')))


def pivot():
    titanic_df = pd.read_csv(FILENAME)
    pvt = titanic_df.pivot_table(
        index=['Sex'],
        columns=['Pclass'],
        values='Name',
        aggfunc='count'
    ).reset_index()

    pvt.to_csv(os.path.join(os.path.expanduser("~"), "loads", 'titanic_pvt.csv'))


with DAG(dag_id='df_pivot',
         default_args=args,
         schedule_interval=None) as dag:
    create_dataset = PythonOperator(
        task_id='download',
        python_callable=download,
        dag=dag
    )

    pivot_dataset = PythonOperator(
        task_id='pivot',
        python_callable=pivot,
        dag=dag
    )

    create_dataset >> pivot_dataset

