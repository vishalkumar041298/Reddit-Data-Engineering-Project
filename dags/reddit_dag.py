import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from pipelines.reddit_pipeline import reddit_pipeline
from pipelines.s3_pipeline import upload_to_s3_pipeline


default_args = {'start_date':datetime(2024, 1, 6), 'owner': 'Vishal'}

file_postfix = datetime.now().strftime('%Y%m%d')


with DAG(dag_id='reddit_etl_dag', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['reddit', 'etl', 'dataengineering']) as dag:

    extraction = PythonOperator(
        task_id='extraction_task',
        python_callable=reddit_pipeline,
        op_kwargs={'filename': f'reddit_{file_postfix}',
                   'sub_reddit': 'dataengineering',
                   'time_filter': 'day',
                   'limit': 100})

    s3_upload = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3_pipeline
    )

    extraction >> s3_upload
