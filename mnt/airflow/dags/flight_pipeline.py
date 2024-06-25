from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

from datetime import datetime, timedelta
import csv
import requests
import json
from datetime import datetime

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("flight_pipeline", start_date=datetime(2023, 10 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    create_datalake_n_warehouse = BashOperator(
        task_id="create_datalake_n_warehouse",
        bash_command="""
            hdfs dfs -mkdir -p /data_warehouse &&
            hdfs dfs -mkdir -p /datalake/bronze &&
            hdfs dfs -mkdir -p /datalake/silver &&
            hdfs dfs -mkdir -p /datalake/gold
        """
    )

    creating_hive_database = HiveOperator(
        task_id="creating_hive_database",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE DATABASE IF NOT EXISTS flight_db; 
            CREATE DATABASE IF NOT EXISTS warehouse_db;
        """
    )

    bronze_tasks = SparkSubmitOperator(
        task_id="bronze_tasks",
        application="/opt/airflow/dags/scripts/ingestToBronze.py",
        name="Ingestion - Postgres to Bronze",
        application_args=['--tblName', 'flights','--jdbcHostname', "postgres", '--jdbcPort', '5432', '--jdbcDatabase', 'airflow_db', \
                '--jdbcUsername', 'airflow', '--jdbcPassword', 'airflow'],
        conn_id="spark_conn",
        executor_memory="6G",
        packages="org.postgresql:postgresql:42.2.23",
        verbose=False
    )

    silver_tasks = SparkSubmitOperator(
        task_id="silver_tasks",
        application="/opt/airflow/dags/scripts/transformToSilver.py",
        name="Transformation - Bronze to Silver",
        application_args=['--tblName', 'flights'],
        conn_id="spark_conn",
        executor_memory="6G",
        # conf={
        #     "spark.jars.repositories" : "org.postgresql:postgresql:42.2.23"
        # },
        packages="org.postgresql:postgresql:42.2.23",
        verbose=False
    )

    gold_tasks = SparkSubmitOperator(
        task_id="gold_tasks",
        application="/opt/airflow/dags/scripts/updateToGold.py",
        application_args=['--silverTableName', 'flights'],
        conn_id="spark_conn",
        name="Transformation - Silver to Gold",
        executor_memory="6G",
        verbose=False
    )

    load_to_warehouse = SparkSubmitOperator(
        task_id="load_to_warehouse",
        application="/opt/airflow/dags/scripts/loadToWarehouse.py",
        application_args=['--tblName', 'flights'],
        conn_id="spark_conn",
        name="Transformation - Silver to Data Warehouse",
        executor_memory="6G",
        verbose=False
    )

create_datalake_n_warehouse >> bronze_tasks >> silver_tasks >> gold_tasks
silver_tasks >> creating_hive_database >> load_to_warehouse