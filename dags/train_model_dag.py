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
    'train_model_dag',
    default_args=default_args,
    description='Entraîner les modèles crypto tous les mois',
    # schedule_interval='0 0 1 * *',  # Exécution le 1er jour de chaque mois à minuit
    schedule_interval=None,  # Exécution temporairement manuelle
) as dag:

    def train_crypto_model(symbol, interval):
        from scripts.train_model import training
        training(symbol, interval)

    train_btc_model_task = PythonOperator(
        task_id='train_btc_model',
        python_callable=train_crypto_model,
        op_kwargs={'symbol': 'BTCUSDT', 'interval': '15m'}
    )

    train_eth_model_task = PythonOperator(
        task_id='train_eth_model',
        python_callable=train_crypto_model,
        op_kwargs={'symbol': 'ETHUSDT', 'interval': '15m'}
    )

    # Ordonnancement des tâches
    train_btc_model_task >> train_eth_model_task
