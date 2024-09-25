from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

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
    'websocket_stream_dag',
    default_args=default_args,
    description='Stream WebSocket data for BTC and ETH',
    schedule_interval='@once',  # Démarre une seule fois mais le service est continu
    catchup=False,
) as dag:

    # Tâche pour démarrer le WebSocket
    def start_websocket_stream():
        from scripts.websocket_stream import start_websocket_streams
        try:
            logging.info("Démarrage du flux WebSocket pour BTC et ETH.")
            start_websocket_streams()
        except Exception as e:
            logging.error(f"Erreur lors du démarrage du flux WebSocket : {e}")
            raise

    # Opérateur Python pour lancer le flux WebSocket
    start_stream_task = PythonOperator(
        task_id='start_websocket_streams',
        python_callable=start_websocket_stream,
    )

    start_stream_task

