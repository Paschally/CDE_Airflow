from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
import json
import requests
import gzip
import shutil
import os


with DAG(
    dag_id="rocket",
    start_date=datetime(2024, 10, 4), 
    schedule_interval=None,
    catchup=False,
) as dag:

    # Step 1: Download the .gz file using PythonOperator
    def download_wikipedia_pageviews():
        url = 'https://dumps.wikimedia.org/other/pageviews/2024/2024-10/pageviews-20241010-160000.gz'
        save_path = f"/home/poikiloth/airflow/airflow_venv/dags/submit/download/pageviews-20241010-160000.gz"
        response = requests.get(url, stream=True)
        with open(save_path, 'wb') as f:
            f.write(response.content)

    download_task = PythonOperator(
        task_id='download_pageviews',
        python_callable=download_wikipedia_pageviews
    )

    # Step 2: BashOperator to extract the .gz file
    extract_task = BashOperator(
        task_id='extract_pageviews',
        bash_command='gzip -d /home/poikiloth/airflow/airflow_venv/dags/submit/download/pageviews-20241010-160000.gz'
    )

    # Step 3: Fetch data and insert into the database
    def fetch_company_pageviews():
        companies = ['Amazon', 'Apple', 'Facebook', 'Google', 'Microsoft']
        pageview_data = {}
        extracted_file = f"/home/poikiloth/airflow/airflow_venv/dags/submit/download/pageviews-20241010-160000"

        
        with open(extracted_file, 'r') as file:
            for line in file:
                for company in companies:
                    if company.lower() in line.lower():
                        parts = line.split(' ')
                        pageview_data[company] = int(parts[2])

        # Write an SQL file to insert the data into the database
        sql_file = f"/home/poikiloth/airflow/airflow_venv/dags/submit/download/insert_pageviews.sql"
        with open(sql_file, 'w') as f:
            for company, views in pageview_data.items():
                f.write(f"INSERT INTO company_pageviews (company, views) VALUES ('{company}', {views});\n")

    fetch_task = PythonOperator(
        task_id='fetch_pageviews',
        python_callable=fetch_company_pageviews
    )

    # Step 4: PostgresOperator to load data into the PostgreSQL database
    load_postgres = PostgresOperator(
        task_id='postgres_load_data',
        postgres_conn_id='postgresql://localhost:3306/test',
        sql="/home/poikiloth/airflow/airflow_venv/dags/submit/download/insert_pageviews.sql"
    )

    # Step 5: Task dependencies
    download_task >> extract_task >> fetch_task >> load_data_task
