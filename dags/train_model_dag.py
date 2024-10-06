from airflow import DAG
from airflow.models import Variable
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

    def train_crypto_model(symbol):
        from scripts.train_model import training
        interval = Variable.get(key='train_model_interval', default_var=None)
        if interval is None:
            Variable.set(key='train_model_interval', value='15m', description='Interval for train_model DAG')
            interval = '15m'

        # start_date = Variable.get(key='train_model_date_start', default_var=None)
        # if start_date is None:
        #     Variable.set(key='train_model_date_start', value='2017-09-01', description='Start date for train_model DAG')
        #     start_date = '2017-09-01'

        # end_date = Variable.get(key='train_model_date_end', default_var=None)
        # if end_date is None:
        #     Variable.set(key='train_model_date_end', value='2017-10-06', description='End date for train_model DAG')
        #     end_date = '2017-10-06'

        # training(symbol, interval, start_date, end_date)
        training(symbol, interval)

    train_btc_model_task = PythonOperator(
        task_id='train_btc_model',
        python_callable=train_crypto_model,
        op_kwargs={'symbol': 'BTCUSDT'}
    )

    train_eth_model_task = PythonOperator(
        task_id='train_eth_model',
        python_callable=train_crypto_model,
        op_kwargs={'symbol': 'ETHUSDT'}
    )

    # Ordonnancement des tâches
    train_btc_model_task >> train_eth_model_task
