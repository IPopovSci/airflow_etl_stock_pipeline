from dag_structure.update_data import update_info
from dag_structure.init_pipeline import initialize_pipeline
from dag_structure.init_postgress_dbs import initialize_postgress
from airflow.decorators import dag
import pendulum

'''This module is created to initialize DAGS.
The dag structure is located in /dag_structure.
This is done to avoid any top-level code for optimal DAG loading'''

default_args = {'owner': 'User', 'conn_id': 'POSTGRES', 'postgres_conn_id': 'POSTGRES', 'database': 'localstorage'}

@dag('init_postgres', schedule='@once', default_args=default_args, start_date=pendulum.now('UTC'),
              render_template_as_native_obj=True, template_searchpath="/opt/airflow/include/sql")
def init_postgres():
    initialize_postgress()

@dag('populate', schedule='@once', default_args=default_args, start_date=pendulum.now('UTC'),
              render_template_as_native_obj=True, template_searchpath="/opt/airflow/include/sql")
def init_dag():
    initialize_pipeline()

@dag('update', schedule='@daily', default_args=default_args, start_date=pendulum.now('UTC'),
              render_template_as_native_obj=True, template_searchpath="/opt/airflow/include/sql")
def update_dag():
    update_info()

globals()['postgres_init_dbs'] = init_postgres()
globals()['populate'] = init_dag()
globals()['update'] = update_dag()