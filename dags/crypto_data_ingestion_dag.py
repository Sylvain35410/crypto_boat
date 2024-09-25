from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import timedelta

# Configuration des paramètres par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Création du DAG
with DAG(
    'crypto_data_ingestion_dag',
    default_args=default_args,
    description='Ingest crypto data every 15 minutes',
    schedule_interval='*/15 * * * *',  # Exécution toutes les 15 minutes
) as dag:

    # Groupe de tâches pour récupérer les données depuis l'API Binance et CoinGecko
    with TaskGroup("fetch_group") as fetch_group:
        
        def fetch_data():
            from scripts.fetch_data import fetch_and_store_all_data
            fetch_and_store_all_data("BTCUSDT")
            fetch_and_store_all_data("ETHUSDT")

        fetch_data_task = PythonOperator(
            task_id='fetch_crypto_data',
            python_callable=fetch_data
        )

    # Groupe de tâches pour stocker les données dans la base de données PostgreSQL
    with TaskGroup("store_group") as store_group:

        def store_data():
            from scripts.store_data import store_historical_data
            # Les données sont déjà stockées dans fetch_data, donc cette tâche peut rester vide
            pass

        store_data_task = PythonOperator(
            task_id='store_crypto_data',
            python_callable=store_data
        )

    # Ordonnancement des tâches
    fetch_group >> store_group




