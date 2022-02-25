import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta



def create_table(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    sql = '''create table public.example as select * from public.department;'''
    pg_hook.run(sql)


def load_data(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_lab')
    sql_load = '''insert into public.example (select * from public.department);'''
    pg_hook.run(sql_load)



args = {
    "depends_on_past": True,
    "start_date": datetime(2022, 2, 25),
    "retries": 0,
    "schedule_interval": "@daily",
	"catchup":True
}

dag = DAG('load-create', default_args=args, schedule_interval='*/2 * * * *')

create_table_task = \
    PythonOperator(task_id='create_table',
                   provide_context=True,
                   python_callable=create_table,
                   dag=dag)

load_data_task = \
    PythonOperator(task_id='load_data',
                   provide_context=True,
                   python_callable=load_data,
                   dag=dag)
