from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
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
    'websocket_stream_dag',
    default_args=default_args,
    description='Stream WebSocket data for BTC and ETH',
    schedule_interval='@once',  # Démarre une seule fois mais le service est continu
    # schedule_interval=None,  # Exécution temporairement manuelle
    catchup=False,
) as dag:

    # Tâche pour démarrer le WebSocket
    def start_websocket_stream(symbol, interval):
        from scripts.websocket_stream import start_websocket_stream_from_binance
        interval = Variable.get(key='websocket_stream_interval', default_var=None)
        if interval is None:
            Variable.set(key='websocket_stream_interval', value='15m', description='Interval for websocket_stream DAG')
            interval = '15m'

        start_websocket_stream_from_binance(symbol, interval)

    # Opérateur Python pour lancer le flux WebSocket
    start_btc_stream_task = PythonOperator(
        task_id='start_btc_stream',
        python_callable=start_websocket_stream,
        op_kwargs={'symbol': 'BTCUSDT', 'interval': '15m'}
    )

    start_eth_stream_task = PythonOperator(
        task_id='start_eth_stream',
        python_callable=start_websocket_stream,
        op_kwargs={'symbol': 'ETHUSDT', 'interval': '15m'}
    )

    # Ordonnancement des tâches
    start_btc_stream_task
    start_eth_stream_task
