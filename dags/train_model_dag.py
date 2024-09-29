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

    # Groupe de tâches pour préparer les données d'entraînement
    with TaskGroup("prepare_data_group") as prepare_data_group:
        
        def prepare_data(symbol):
            from scripts.train_model import load_training_data
            load_training_data(symbol, "15m")

        # Préparer les données pour BTC et ETH
        prepare_btc_data_task = PythonOperator(
            task_id='prepare_btc_training_data',
            python_callable=lambda: prepare_data("BTCUSDT")
        )

        prepare_eth_data_task = PythonOperator(
            task_id='prepare_eth_training_data',
            python_callable=lambda: prepare_data("ETHUSDT")
        )

    # Groupe de tâches pour entraîner le modèle
    with TaskGroup("train_model_group") as train_model_group:

        def train_crypto_model(symbol):
            from scripts.train_model import train_model
            train_model(symbol, "15m")

        train_btc_model_task = PythonOperator(
            task_id='train_btc_model',
            python_callable=lambda: train_crypto_model("BTCUSDT")
        )

        train_eth_model_task = PythonOperator(
            task_id='train_eth_model',
            python_callable=lambda: train_crypto_model("ETHUSDT")
        )

    # Groupe de tâches pour sauvegarder le modèle entraîné
    with TaskGroup("save_model_group") as save_model_group:

        def save_model(symbol):
            from scripts.train_model import train_and_save_model
            train_and_save_model(symbol, "15m")

        save_btc_model_task = PythonOperator(
            task_id='save_btc_trained_model',
            python_callable=lambda: save_model("BTCUSDT")
        )

        save_eth_model_task = PythonOperator(
            task_id='save_eth_trained_model',
            python_callable=lambda: save_model("ETHUSDT")
        )

    # Ordonnancement des tâches
    prepare_btc_data_task >> train_btc_model_task >> save_btc_model_task
    prepare_eth_data_task >> train_eth_model_task >> save_eth_model_task
