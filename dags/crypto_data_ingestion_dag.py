from airflow import DAG
from airflow.exceptions import AirflowFailException
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
    # schedule_interval='*/15 * * * *',  # Exécution toutes les 15 minutes
    schedule_interval=None,  # Exécution toutes les 15 minutes
) as dag:

    # Groupe de tâches pour récupérer les données depuis CoinGecko
    with TaskGroup("fetch_coin_gecko") as fetch_coin_gecko:
        def fetch_coin_gecko_data(symbol):
            from scripts.fetch_data import fetch_coin_gecko_data
            try:
                fetch_coin_gecko_data(symbol)
            except Exception as error:
                raise

        fetch_crypto_BTCUSDT = PythonOperator(
            task_id='fetch_crypto_BTCUSDT',
            python_callable=fetch_coin_gecko_data,
            op_kwargs={'symbol': 'BTCUSDT'}
        )

        fetch_crypto_ETHUSDT = PythonOperator(
            task_id='fetch_crypto_ETHUSDT',
            python_callable=fetch_coin_gecko_data,
            op_kwargs={'symbol': 'ETHUSDT'}
        )

    # Groupe de tâches pour récupérer les données depuis l'API Binance
    with TaskGroup("fetch_binance") as fetch_binance:
        def fetch_binance_data(symbol):
            from scripts.fetch_data import fetch_binance_data
            try:
                fetch_binance_data(symbol, interval="15m", start_date="2017-09-01", end_date="2017-10-05")
            except Exception as error:
                raise

        fetch_binance_BTCUSDT = PythonOperator(
            task_id='fetch_binance_BTCUSDT',
            python_callable=fetch_binance_data,
            op_kwargs={'symbol': 'BTCUSDT'}
        )

        fetch_binance_ETHUSDT = PythonOperator(
            task_id='fetch_binance_ETHUSDT',
            python_callable=fetch_binance_data,
            op_kwargs={'symbol': 'ETHUSDT'}
        )

    # Ordonnancement des tâches
    fetch_crypto_BTCUSDT >> fetch_binance_BTCUSDT
    fetch_crypto_ETHUSDT >> fetch_binance_ETHUSDT
