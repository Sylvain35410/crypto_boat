from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# Définir les paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),  # Utilisation de l'heure actuelle
    'retries': 1,
    'retry_delay': timedelta(minutes=1),  # Délai avant retry en cas d'échec
}

# Chemin du fichier dashboard.py
DASHBOARD_SCRIPT_PATH = '/opt/airflow/app/dashboard.py'

# Création du DAG
with DAG(
    dag_id='dashboard_dag',
    default_args=default_args,
    description='DAG pour lancer le dashboard Dash',
    schedule_interval='@once',  # Démarre une seule fois mais le service est continu
    # schedule_interval=None,  # Exécution manuelle
    catchup=False
) as dag:

    # Utilisation de TaskGroup pour organiser les tâches
    with TaskGroup("dashboard_group") as dashboard_group:
        
        # Tâche Bash pour lancer le serveur Dash avec Python
        run_dashboard = BashOperator(
            task_id='run_dashboard_task',
            # Lancer le fichier dashboard.py
            bash_command=f'python3 {DASHBOARD_SCRIPT_PATH}',
            do_xcom_push=False
        )

    # Exécuter le groupe de tâches
    dashboard_group
