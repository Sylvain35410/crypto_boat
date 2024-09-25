import psycopg2
import os
import pickle
import csv
import hashlib
import pandas as pd

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@localhost:5432/cryptoboat_db')

# Vérifier la santé de la base de données
def check_database_health():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection error: {e}")
        return False

# Charger le modèle pré-entraîné
def load_model():
    try:
        model_path = '/opt/airflow/model/model_crypto_rfc.pkl'  # Chemin du modèle
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        return model
    except Exception as e:
        raise RuntimeError(f"Failed to load model: {e}")

# Faire une prédiction à partir du modèle
def make_prediction(model):
    # Code pour effectuer une prédiction
    pass

# Hacher le mot de passe pour le stockage
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Ajouter un utilisateur au fichier CSV
def add_user_to_csv(username, email, password):
    try:
        csv_path = '/opt/airflow/users/users.csv'
        hashed_password = hash_password(password)
        with open(csv_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([username, email, hashed_password])
    except Exception as e:
        raise RuntimeError(f"Failed to add user: {e}")

# Importer les utilisateurs depuis le CSV vers PostgreSQL
def import_users_from_csv():
    try:
        csv_path = '/opt/airflow/users/users.csv'
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        with open(csv_path, newline='') as file:
            reader = csv.reader(file)
            imported_count = 0
            for row in reader:
                cursor.execute(
                    "INSERT INTO users (username, email, password_hash) VALUES (%s, %s, %s)",
                    (row[0], row[1], row[2])
                )
                imported_count += 1
            conn.commit()
            conn.close()
            return imported_count
    except Exception as e:
        raise RuntimeError(f"Failed to import users: {e}")

# Récupérer le prix actuel du stream depuis PostgreSQL
def get_current_stream_price(symbol):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        query = '''
            SELECT close_price
            FROM stream_crypto_data
            WHERE id_crypto_characteristics = %s
            ORDER BY event_time DESC
            LIMIT 1
        '''
        cursor.execute(query, (symbol,))
        current_price = cursor.fetchone()
        conn.close()
        if current_price:
            return current_price[0]
        else:
            return None
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve current stream price: {e}")

# Récupérer le prix de clôture de la bougie précédente
def get_previous_closing_price(symbol):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        query = '''
            SELECT close_price
            FROM historical_crypto_data
            WHERE id_crypto_characteristics = %s
            ORDER BY open_time DESC
            LIMIT 1
        '''
        cursor.execute(query, (symbol,))
        previous_price = cursor.fetchone()
        conn.close()
        if previous_price:
            return previous_price[0]
        else:
            return None
    except Exception as e:
        raise RuntimeError(f"Failed to retrieve previous closing price: {e}")


