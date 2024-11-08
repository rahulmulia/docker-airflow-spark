# import airflow
# from datetime import timedelta
# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from airflow.utils.dates import days_ago
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from get_spotify_data import main_spotify
# from get_youtube_data import main_youtube
#
#
# # Define Python functions for our tasks
# def get_data_spotify():
#     """Simulate fetching data"""
#     main_spotify()
#     return "Data fetched successfully"
#
# def get_data_youtube():
#     """Simulate fetching data"""
#     main_youtube()
#     return "Data fetched successfully"
#
#
# default_args = {
#     'owner': 'airflow',
#     'retry_delay': timedelta(minutes=5),
# }
#
# DAG = DAG(
#         dag_id = "spark_airflow_dag",
#         default_args=default_args,
#         schedule_interval=None,
#         dagrun_timeout=timedelta(minutes=60),
#         description='use case of sparkoperator in airflow',
#         start_date = airflow.utils.dates.days_ago(1)
# )
#
#
# # Create tasks using PythonOperator
# fetch_data_spotify = PythonOperator(
#     task_id='fetch_data_spotify',
#     python_callable=get_data_spotify,
#     dag=DAG
# )
#
#
# # Create tasks using PythonOperator
# fetch_data_youtube = PythonOperator(
#     task_id='fetch_data_youtube',
#     python_callable=get_data_youtube,
#     dag=DAG
# )
#
#
# check_mode_spotify = BashOperator(
#     task_id='check_mode_spotify',
#     bash_command='chmod +x upload_spotify.sh',
#     dag=DAG
# )
#
# check_mode_youtube = BashOperator(
#     task_id='check_mode_youtube',
#     bash_command='chmod +x upload_youtube.sh',
#     dag=DAG
# )
#
#
# # Running a shell script
# upload_starrocksDB_spotify = BashOperator(
#     task_id='upload_starrocksDB_spotify',
#     # Make sure the script is executable (chmod +x)
#     bash_command='upload_spotify.sh',
#     dag=DAG
# )
#
#
# # Running a shell script
# upload_starrocksDB_youtube = BashOperator(
#     task_id='upload_starrocksDB_youtube',
#     # Make sure the script is executable (chmod +x)
#     bash_command='upload_youtube.sh',
#     dag=DAG
# )
#
#
# # Set up dependencies
# [fetch_data_spotify, fetch_data_youtube] >> check_mode_spotify >> check_mode_youtube >> [upload_starrocksDB_spotify, upload_starrocksDB_youtube]

import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
        dag_id = "spark_airflow_dag",
        default_args=default_args,
        schedule_interval=None,
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

Extract = SparkSubmitOperator(
		application = "/opt/airflow/dags/spark_etl_script_docker.py",
		conn_id= 'spark_local',
		task_id='spark_submit_task',
		dag=spark_dag
		)

Extract