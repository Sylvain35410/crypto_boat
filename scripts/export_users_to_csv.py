import psycopg2
import csv
import os
import logging

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@localhost:5432/cryptoboat_db')

# Fonction pour exporter les utilisateurs depuis la base de données PostgreSQL vers un fichier CSV
def export_users_to_csv():
    connection = None
    try:
        connection = psycopg2.connect(DATABASE_URL)
        cursor = connection.cursor()

        # Requête pour récupérer les utilisateurs
        query = "SELECT id_users, username, email, password_hash, created_at FROM users"
        cursor.execute(query)
        users = cursor.fetchall()

        # Définir le chemin du fichier CSV dans le dossier `users/`
        csv_file_path = os.path.join('/opt/airflow/users', 'users.csv')

        # Écriture des utilisateurs dans le fichier CSV
        with open(csv_file_path, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Écrire les en-têtes
            csv_writer.writerow(['id_users', 'username', 'email', 'password_hash', 'created_at'])
            # Écrire les données des utilisateurs
            csv_writer.writerows(users)

        logging.info(f"Export des utilisateurs vers {csv_file_path} réussi.")
    
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erreur lors de l'export des utilisateurs : {error}")
    
    finally:
        if connection is not None:
            connection.close()

if __name__ == '__main__':
    export_users_to_csv()

