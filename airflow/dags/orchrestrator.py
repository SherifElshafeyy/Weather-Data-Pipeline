import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount 
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule


sys.path.append('/opt/airflow/api-request')
from insert_record import main, cleanup_failed_run,update_run_status,is_api_available


with DAG(
    dag_id="weather_api_orcherastrator",
    start_date=datetime(2025, 8, 25),
    schedule=timedelta(minutes=10),
    catchup=False,
    tags=["weather", "dbt", "etl"],
) as dag:

    #wait for api response task
    wait_for_api = PythonSensor(
       task_id="wait_for_api",
        python_callable=is_api_available,
        poke_interval=30,
        timeout=300,
        mode="reschedule"  
    )
    #start ingesting data
    ingest_data = PythonOperator(
        task_id="ingest_data",
        python_callable=main,
        op_kwargs={"run_id": "{{ run_id }}"}
    )
    #dbt transform data
    dbt_transform_data = DockerOperator(
        task_id="dbt_transform_data",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command= "run --vars '{run_id: {{ run_id }}}'",
        working_dir="/usr/app",
        mounts=[
            Mount(
                source="/home/shree/repos/weather-data-project/dbt/my_project",
                target="/usr/app/",
                type="bind"
            ),
            Mount(
                source="/home/shree/repos/weather-data-project/dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind"
            )
        ],
        network_mode="weather-data-project_my-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success"
    )

    #dbt test data
    dbt_test = DockerOperator(
        task_id="dbt_test",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="test --vars '{run_id: {{ run_id }}}'",  # <-- runs dbt tests
        working_dir="/usr/app",
        mounts=[
            Mount(
                source="/home/shree/repos/weather-data-project/dbt/my_project",
                target="/usr/app/",
                type="bind"
            ),
            Mount(
                source="/home/shree/repos/weather-data-project/dbt/profiles.yml",
                target="/root/.dbt/profiles.yml",
                type="bind"
            )
        ],
        network_mode="weather-data-project_my-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove="success"
    )

    #update status of valif recotds in control_table
    update_status_valid = PythonOperator(
    task_id="update_status_valid",
    python_callable=update_run_status,
    op_kwargs={"run_id": "{{ run_id }}", "status": "valid"},
    trigger_rule="all_success"
    )
    #update status of invalid record in control_table
    update_status_invalid = PythonOperator(
    task_id="update_status_invalid",
    python_callable=update_run_status,
    op_kwargs={"run_id": "{{ run_id }}", "status": "invalid"},
    trigger_rule="one_failed"
    )


    # Cleanup Task (only runs if dbt_test fails) from weather_cleansed_table
    cleanup_failed = PythonOperator(
        task_id="cleanup_failed",
        python_callable=cleanup_failed_run,
        op_kwargs={"run_id": "{{ run_id }}"},
        trigger_rule="one_failed"  # Runs only if dbt_test fails
    )

    # Dependencies
    wait_for_api>>ingest_data >> dbt_transform_data >> dbt_test>> [update_status_valid,update_status_invalid,cleanup_failed]

     

    
    

    





    