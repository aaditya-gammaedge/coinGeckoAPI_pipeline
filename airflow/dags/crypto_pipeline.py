# from airflow import DAG
# # from airflow.operators.python import PythonOperator
# from airflow.providers.standard.operators.python import PythonOperator
# from datetime import datetime
# import requests

# from include.crypto.extract import fetch_top_50_coins

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime ,timedelta



from include.crypto.extract import fetch_top_50_coins
from include.crypto.transform import transform_crypto_data
from include.crypto.load import load_to_postgres



with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["crypto"],
) as dag:




    fetch_task = PythonOperator(
        task_id="fetch_top_50",
        python_callable=fetch_top_50_coins,
        email_on_failure=True,     
        email_on_retry=True,
        retries=1,                 
        retry_delay=timedelta(minutes=5)
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_crypto_data,
        email_on_failure=True,     
        email_on_retry=True,
        retries=1,                 
        retry_delay=timedelta(minutes=5)
    )


    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        email_on_failure=True,     
        email_on_retry=True,
        retries=1,                 
        retry_delay=timedelta(minutes=5)
    )





    fetch_task >> transform_task >> load_task
   

