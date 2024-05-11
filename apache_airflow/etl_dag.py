# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Roman Hoehn',
    'start_date': days_ago(0),
    'email': ['rohoehn123@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'lab_dag',
    default_args=default_args,
    description='Practice DAG for IBM Data Engineering Certificate',
    schedule_interval=timedelta(minutes==1),
)

link="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"

download = BashOperator(
    task_id="download_file",
    bash_command=f"curl f{link} --output /home/project/airflow/dags/downloaded-data.txt"
    dag = dag

)

extract = BashOperator(
    task_id='extract',
    bash_command='cut -d"#" -f1,4 /home/project/airflow/dags/downloaded-data.txt > /home/project/airflow/dags/extracted-data.txt'
    dag=dag,
)
# define the second task
transform = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv',
    dag=dag,
)
# task pipeline
extract >> transform_and_load