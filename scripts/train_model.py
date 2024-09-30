import logging
import os
import numpy as np
import pandas as pd
import pickle
import psycopg2
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import GridSearchCV, train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from scripts.lib_sql import get_id_crypto_characteristics, get_id_interval, get_historical_data_to_df

# Configuration de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Fonction pour charger les données historiques depuis PostgreSQL
def load_training_data(symbol, interval):
    logging.info(f"Chargement des données historiques.")
    try:
        id_interval = get_id_interval( interval )
        id_symbol   = get_id_crypto_characteristics(symbol)
        result      = get_historical_data_to_df(id_symbol, id_interval)
        # result = result[:500]
    except Exception as e:
        raise

    if result.shape[0] == 0:
        raise Exception(f"Aucune donnée trouvée.")

    return result

# Fonction de préparation des données pour l'entraînement et de test
def prepare_data(data):
    logging.info("Préparation des données d'entraînement et de test.")

    features = data.drop('close_price', axis=1)
    target   = data['close_price']

    # Partage des données
    X_train, X_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

    # Standardisation des variables numériques à l'aide de StandardScaler en estimant les paramètres sur le jeu d'entraînement et en l'appliquant sur le jeu d'entraînement et de test.
    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test  = sc.transform(X_test)

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

# Fonction pour entraîner le modèle
def train_model(X_train, X_test, y_train, y_test):
    logging.info(f"Entraînement du modèle RandomForestRegressor.")
    best_model = perform_grid_search(X_train, y_train)

    # Prédiction
    y_pred_random_forest_train = best_model.predict(X_train)
    y_pred_random_forest_test  = best_model.predict(X_test)

    # Calcul des métriques
    rmse_random_forest_train = mean_squared_error( y_train, y_pred_random_forest_train, squared=False)
    rmse_random_forest_test  = mean_squared_error( y_test,  y_pred_random_forest_test, squared=False)

    # Calcul des erreurs RMSE pour l'entraînement et le test
    logging.info(f"RMSE d'entraînement : {rmse_random_forest_train:.3f}, RMSE de test : {rmse_random_forest_test:.3f}")

    return best_model

# Fonction pour sauvegarder le modèle
def save_model(symbol, interval, model):
    # Sauvegarde du modèle avec un nom unique
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = f"/opt/airflow/model/{symbol}_{interval}_{current_date}.pkl"

    # Sauvegarde du modèle dans un fichier pickle
    try:
        with open(model_path, 'wb') as model_file:
            pickle.dump(model, model_file)
        logging.info(f"Modèle sauvegardé sous {model_path}.")
    except Exception as e:
        logging.error(f"Erreur lors de la sauvegarde du modèle : {e}")

# Fonction principale pour l'entraînement du modèle
def training(symbol, interval):
    logging.info(f"Début de l'entraînement pour {symbol} avec interval {interval}.")

    # Chargement des données historiques
    features = load_training_data(symbol, interval)

    # Préparation des données pour l'entraînement et de test
    X_train, X_test, y_train, y_test = prepare_data(features)

    # Entraînement du modèle
    model = train_model(X_train, X_test, y_train, y_test)

    # Sauvegarde du modèle
    save_model(symbol, interval, model)

    logging.info(f"Entraînement terminé pour {symbol} avec interval {interval}.")

# Exemple d'appel
if __name__ == "__main__":
    training("BTCUSDT", "15m")
