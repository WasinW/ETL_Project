import json
from datetime import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
#from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

#import requests
import pandas as pd


def init_table():
    create_table_sql_query = """ 
    DROP SCHEMA ingestion;
    CREATE SCHEMA ingestion;

    DROP TABLE IF EXISTS ingestion.order_detail;
    CREATE TABLE ingestion.order_detail (
    	order_created_timestamp timestamp NOT NULL ,
    	status text NOT NULL,
    	price integer  NOT NULL,
    	discount float8  ,
    	id text NOT NULL,
    	driver_id text NOT NULL,
    	user_id text ,
    	restaurant_id text NOT NULL
    )
    """
    postgres = PostgresHook(postgres_conn_id='airflow')
    postgres.run(create_table_sql_query)
    #create_table = PostgresOperator(
    #sql = create_table_sql_query,
    #task_id = "create_table_task",
    #postgres_conn_id = "postgres_local",
    #dag = dag_psql
    #)





def save_data_into_db():
    order_detail = pd.read_csv('/usr/local/airflow/dags/data/order_detail.csv')
    restaurant_detail = pd.read_csv('/usr/local/airflow/dags/data/restaurant_detail.csv')
    postgres = PostgresHook(postgres_conn_id='airflow_db')
    insert = """
        INSERT INTO ingestion.order_detail (
            order_created_timestamp 
            , status 
            , price 
            , discount 
            , id 
            , driver_id 
            , user_id 
            , restaurant_id )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    print(insert, parameters=(
                                    order_detail['order_created_timestamp'],
                                    order_detail['status'],
                                    order_detail['price'],
                                    order_detail['discount'],
                                    order_detail['id'],
                                    order_detail['driver_id'],
                                    order_detail['user_id'],
                                    order_detail['restaurant_id']
                                    ))
#    postgres.run(insert, parameters=(
#                                    order_detail['order_created_timestamp'],
#                                    order_detail['status'],
#                                    order_detail['price'],
#                                    order_detail['discount'],
#                                    order_detail['id'],
#                                    order_detail['driver_id'],
#                                    order_detail['user_id'],
#                                    order_detail['restaurant_id']
#                                    ))



default_args = {
    'owner': 'wasin',
    'start_date': datetime(2020, 7, 1),
    'email': ['wasin.56050128@gmail.com'],
}

#def get_data():
#    url = 'https://covid19.th-stat.com/api/open/today'
#    response = requests.get(url)
#    data = response.json()
#    with open('data.json', 'w') as f:
#        json.dump(data, f)
#
#    return data


with DAG('ingestion_test',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline for COVID-19 report',
         catchup=False) as dag:

    t1 = PythonOperator(
        task_id='init_table',
        python_callable=init_table
    )

    t2 = PythonOperator(
        task_id='save_data_into_db',
        python_callable=save_data_into_db
    )

    t3 = EmailOperator(
        task_id='send_email',
        to=['zkan@hey.team'],
        subject='Your COVID-19 report today is ready',
        html_content='Please check your dashboard. :)'
    )

    t1 >> t2 >> t3
