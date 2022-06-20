import os
import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from utils.utils import super_logger


args = {
    'owner': 'admin',
    'start_date': dt.datetime(2022, 1, 1),
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False
}

CONFIG = "test.txt"


def get_creds(filename):

    with open(filename, 'r', encoding='utf-8') as f:
        data = f.readlines()


with DAG(dag_id='read_local_config',
         default_args=args,
         schedule_interval=None) as dag:

    get_creds_abs_path = PythonOperator(
        task_id='get_creds_abs_path',
        python_callable=get_creds,
        op_kwargs={
            'filename': CONFIG
        }
    )

    get_creds_rel_path = PythonOperator(
        task_id='get_creds_rel_path',
        python_callable=get_creds,
        op_kwargs={
            'filename': os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG)
        }
    )

    get_pwd = BashOperator(
        task_id='get_pwd',
        bash_command='pwd'
    )

    get_pwd >> [get_creds_abs_path, get_creds_rel_path]


if __name__ =='__main__':
    get_creds(CONFIG)
