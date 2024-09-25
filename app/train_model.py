import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV, train_test_split
import pickle
import logging
import os

# Configuration de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fonction pour charger les données historiques depuis PostgreSQL
def load_training_data(symbol, interval):
    logging.info(f"Chargement des données historiques pour {symbol} avec interval {interval}.")
    
    connection = None
    try:
        connection = psycopg2.connect(
            dbname="crypto_data",
            user="airflow",
            password="airflow",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()

        # Requête pour récupérer les données historiques
        query = '''
            SELECT open_price, high_price, low_price, close_price, volume
            FROM historical_crypto_data
            WHERE id_crypto_characteristics = (SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = %s)
            AND id_interval = (SELECT id_interval FROM intervals WHERE intervals = %s)
            ORDER BY open_time DESC
        '''
        cursor.execute(query, (symbol, interval))
        result = cursor.fetchall()
        cursor.close()

        if result:
            df = pd.DataFrame(result, columns=['open_price', 'high_price', 'low_price', 'close_price', 'volume'])
            logging.info(f"Données chargées avec succès pour {symbol}.")
            target = df['close_price']
            return df, target
        else:
            logging.warning(f"Aucune donnée trouvée pour {symbol} avec interval {interval}.")
            return None, None
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erreur lors du chargement des données : {error}")
        return None, None
    finally:
        if connection is not None:
            connection.close()

# Fonction de préparation des données pour l'entraînement
def prepare_data(features, target):
    logging.info("Préparation des données d'entraînement et de test.")
    features = features[['open_price', 'high_price', 'low_price', 'close_price', 'volume']]
    X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test

# Fonction de recherche des meilleurs hyperparamètres avec GridSearch
def perform_grid_search(X_train, y_train):
    logging.info("Recherche des meilleurs hyperparamètres pour RandomForest.")
    param_grid = {
        'n_estimators': [100, 200, 500],
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2]
    }
    model = RandomForestRegressor(random_state=42)
    grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1)
    grid_search.fit(X_train, y_train)
    return grid_search.best_estimator_

# Fonction pour entraîner et sauvegarder le modèle
def train_and_save_model(symbol, interval, X_train, X_test, y_train, y_test):
    logging.info(f"Entraînement du modèle pour {symbol} avec interval {interval}.")
    best_model = perform_grid_search(X_train, y_train)

    # Calcul des erreurs RMSE pour l'entraînement et le test
    train_rmse = np.sqrt(mean_squared_error(y_train, best_model.predict(X_train)))
    test_rmse = np.sqrt(mean_squared_error(y_test, best_model.predict(X_test)))
    logging.info(f"RMSE d'entraînement : {train_rmse:.3f}, RMSE de test : {test_rmse:.3f}")

    # Sauvegarde du modèle avec un nom unique
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_filename = f"{symbol}_{interval}_{current_date}.pkl"
    model_directory = '/opt/airflow/model/'
    if not os.path.exists(model_directory):
        os.makedirs(model_directory)
    model_path = os.path.join(model_directory, model_filename)

    # Sauvegarde du modèle dans un fichier pickle
    try:
        with open(model_path, 'wb') as model_file:
            pickle.dump(best_model, model_file)
        logging.info(f"Modèle sauvegardé sous {model_path}.")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde du modèle : {e}")

# Fonction principale pour l'entraînement du modèle
def train_model(symbol, interval):
    logging.info(f"Début de l'entraînement pour {symbol} avec interval {interval}.")
    features, target = load_training_data(symbol, interval)
    
    if features is None or target is None:
        logging.error(f"Impossible d'entraîner le modèle pour {symbol} avec interval {interval} à cause de données insuffisantes.")
        return
    
    # Préparation des données d'entraînement et de test
    X_train, X_test, y_train, y_test = prepare_data(features, target)
    
    # Entraînement et sauvegarde du modèle
    train_and_save_model(symbol, interval, X_train, X_test, y_train, y_test)
    logging.info(f"Entraînement terminé pour {symbol} avec interval {interval}.")

# Exemple d'appel
if __name__ == "__main__":
    symbol = "BTCUSDT"  # Vous pouvez changer pour "ETHUSDT"
    interval = "15m"  # L'intervalle peut être "15m", "1h", "4h", etc.
    train_model(symbol, interval)


