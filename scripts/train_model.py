import logging
import os
import numpy as np
import pandas as pd
import pickle
import time
from datetime import datetime
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV, train_test_split
from scripts.lib_sql import get_historical_data, get_id_interval, get_id_crypto_characteristics

# Configuration de logging :


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Récupération des données de la DB: 


def load_training_data(symbol, interval="15m", lag_count=2):
    """
    Fonction pour charger les données d'entraînement depuis la base de données.

    Arguments :
    - symbol (str) : Le symbole de la cryptomonnaie (par ex. 'BTCUSDT').
    - interval (str) : L'intervalle de temps pour les données (par ex. '15m').
    - lag_count (int) : Nombre de décalages (lags) à inclure (défaut = 2).

    Description :
    - La fonction utilise `get_historical_data` pour récupérer les données historiques avec ou sans décalages (lags).
    - Elle lève une exception si aucune donnée n'est trouvée.

    Retour :
    - Les features (feats) et la cible (target) pour l'entraînement du modèle.
    """
    logging.info(f"Loading historical data for {symbol} with interval {interval}.")  
    try:
        # Utilisation de la fonction fusionnée get_historical_data
        id_interval   = get_id_interval(interval)
        id_symbol     = get_id_crypto_characteristics(symbol)
        feats, target = get_historical_data(id_symbol, id_interval, lags=True, limit=lag_count)
    except Exception as e:
        raise RuntimeError(f"Error loading historical data: {str(e)}")

    if feats.shape[0] == 0:
        raise Exception(f"No data found for {symbol} with interval {interval}.")  

    return feats, target


# Préparation des données pour l'entraînement :


def prepare_data(feats, target):
    """
    Préparation des données pour l'entraînement et le test.

    Arguments :
    - feats (DataFrame) : Les features (caractéristiques) extraites des données historiques.
    - target (Series) : La cible (prix de clôture suivant).

    Description :
    - Sépare les données en jeux d'entraînement et de test en utilisant un ratio 80/20.
    
    Retour :
    - X_train, X_test, y_train, y_test : Les données d'entraînement et de test pour le modèle.
    """
    logging.info("Preparing training and test data.")  

    # Séparation des données en jeux d'entraînement et de test
    X_train, X_test, y_train, y_test = train_test_split(feats, target, test_size=0.2, random_state=42)

    return X_train, X_test, y_train, y_test


# Recherche des hyperparamètres : 


def perform_grid_search(X_train, y_train):
    """
    Recherche des meilleurs hyperparamètres pour le modèle RandomForestRegressor.

    Arguments :
    - X_train (DataFrame) : Les features d'entraînement.
    - y_train (Series) : Les cibles d'entraînement.

    Description :
    - Utilise GridSearchCV pour rechercher les meilleurs hyperparamètres pour le modèle RandomForest.

    Retour :
    - Le meilleur modèle trouvé par GridSearchCV.
    """
    logging.info("Searching for best hyperparameters for RandomForest.")  

    param_grid = {
        'n_estimators': [100, 200, 500],
        'max_depth': [None, 10, 20],
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2]
    }
    rf_model = RandomForestRegressor(random_state=42)
    grid_search = GridSearchCV(estimator=rf_model, param_grid=param_grid, cv=5, scoring='neg_mean_squared_error', n_jobs=-1, verbose=2)
    grid_search.fit(X_train, y_train)
    
    logging.info(f"Best model parameters: {grid_search.best_params_}")
    logging.info(f"Best model score: {grid_search.best_score_}")
    
    return grid_search.best_estimator_


# Entraînement du modèle et calcul des métriques: 


def train_model(X_train, X_test, y_train, y_test):
    """
    Entraînement du modèle RandomForestRegressor et calcul des métriques de performance.

    Arguments :
    - X_train (DataFrame) : Les données d'entraînement.
    - X_test (DataFrame) : Les données de test.
    - y_train (Series) : Les cibles d'entraînement.
    - y_test (Series) : Les cibles de test.

    Description :
    - Entraîne le modèle sur les données d'entraînement.
    - Effectue des prédictions sur les jeux d'entraînement et de test, puis calcule les erreurs RMSE.

    Retour :
    - Le modèle entraîné.
    """
    logging.info(f"Training RandomForestRegressor model.")  
    best_model = perform_grid_search(X_train, y_train)

    # Prédictions
    y_pred_random_forest_train = best_model.predict(X_train)
    y_pred_random_forest_test = best_model.predict(X_test)

    # Calcul des erreurs RMSE
    rmse_random_forest_train = np.sqrt(mean_squared_error(y_train, y_pred_random_forest_train))
    rmse_random_forest_test = np.sqrt(mean_squared_error(y_test, y_pred_random_forest_test))

    # Log des erreurs RMSE
    logging.info(f"Training RMSE: {rmse_random_forest_train:.3f}, Test RMSE: {rmse_random_forest_test:.3f}")  

    return best_model


# Sauvegarde du modèle :

def save_model(symbol, interval, model):
    """
    Sauvegarde le modèle entraîné sous forme de fichier pickle.

    Arguments :
    - symbol (str) : Le symbole de la cryptomonnaie.
    - interval (str) : L'intervalle de temps utilisé pour l'entraînement.
    - model : Le modèle entraîné à sauvegarder.

    Description :
    - Sauvegarde le modèle dans un fichier pickle avec un nom basé sur le symbole, l'intervalle, et la date actuelle.

    Retour :
    - Aucun.
    """
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = f"/opt/airflow/model/{symbol}_{interval}_{current_date}.pkl"

    # Sauvegarde du modèle dans un fichier pickle
    try:
        with open(model_path, 'wb') as model_file:
            pickle.dump(model, model_file)
        logging.info(f"Model saved at {model_path}.") 
    except Exception as e:
        logging.error(f"Error saving the model: {e}")  


# Fonction principale d'entraînement:

def training(symbol, interval="15m", lag_count=2):
    """
    Fonction principale pour l'entraînement du modèle.

    Arguments :
    - symbol (str) : Le symbole de la cryptomonnaie (par ex. 'BTC').
    - interval (str) : L'intervalle de temps (par ex. '15m').
    - lag_count (int) : Nombre de décalages à utiliser pour les données (par défaut = 2).

    Description :
    - Vérifie si un modèle récent existe déjà. Si oui, il est utilisé.
    - Charge les données historiques, prépare les données pour l'entraînement, entraîne le modèle, et le sauvegarde.

    Retour :
    - Aucun.
    """
    logging.info(f"Starting training for {symbol} with interval {interval}.")  

    # Vérification si un modèle plus récent existe
    model_path = f"/opt/airflow/model/{symbol}_{interval}.pkl"
    if os.path.exists(model_path):
        model_age_days = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(model_path))).days
        if model_age_days <= 30:
            logging.info(f"Using existing model for {symbol} ({interval}), age: {model_age_days} days.")  
            return

    # Chargement des données historiques
    feats, target = load_training_data(symbol, interval, lag_count)

    # Préparation des données pour l'entraînement et de test
    X_train, X_test, y_train, y_test = prepare_data(feats, target)

    # Entraînement du modèle
    model = train_model(X_train, X_test, y_train, y_test)

    # Sauvegarde du modèle
    save_model(symbol, interval, model)

    logging.info(f"Training completed for {symbol} with interval {interval}.")  
