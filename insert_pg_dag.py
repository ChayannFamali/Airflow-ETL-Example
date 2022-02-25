import airflow
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

args = {
    "depends_on_past": True,
    "start_date": datetime(2022, 2, 25),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "schedule_interval": "@daily",
	"catchup":True
}



def insert_pg():
    """ Inserts datas into the database """

    insert_query = """
        	INSERT INTO public.my_table(UUID_FlowFile,id_record,short_name,id_request)
	        SELECT * FROM source_schema.my_table;
        """
    pg_hook = PostgresHook(postgres_conn_id="postgre_dwh_test")
    pg_hook.run(insert_query)


with DAG("insert_pg", default_args=args) as dag:
    
    Task_I = PythonOperator(
        task_id="insert_pg", python_callable=insert_pg
    )        

