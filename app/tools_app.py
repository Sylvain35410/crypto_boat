import os
import pickle
from datetime import datetime
from scripts.lib_sql import get_stream_price_and_next_time, get_historical_data, get_id_interval, get_id_crypto_characteristics

# Charger le modèle pré-entraîné
def load_model(symbol, interval):
    """
    Fonction pour charger un modèle pré-entraîné de RandomForest pour la cryptomonnaie spécifiée et l'intervalle de temps donné.

    Arguments :
    - `symbol` (str) : Le symbole de la cryptomonnaie (par exemple, "BTC").
    - `interval` (str) : L'intervalle de temps (par exemple, "15m").

    Retour :
    - Le modèle RandomForestRegressor chargé à partir du fichier pickle.
    """
    try:
        model_path = f'/model/{symbol}_{interval}.pkl'  # Chemin du modèle
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model for {symbol} with interval {interval} not found.")
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        return model
    except Exception as e:
        raise RuntimeError(f"Failed to load model: {e}")

# Comparer les prix et déterminer une décision Buy/Sell/Hold
def make_decision(actual_price, predicted_price):
    """
    Fonction pour déterminer la décision d'investissement basée sur la prédiction du modèle.

    Arguments :
    - `actual_price` (float) : Le prix actuel de la cryptomonnaie.
    - `predicted_price` (float) : Le prix prédit de la cryptomonnaie.

    Retour :
    - `Buy` si le prix prédit est supérieur au prix actuel.
    - `Sell` si le prix prédit est inférieur au prix actuel.
    - `Hold` si le prix prédit est égal au prix actuel.
    """
    if predicted_price > actual_price:
        return "Buy"
    elif predicted_price < actual_price:
        return "Sell"
    else:
        return "Hold"

# Faire une prédiction et prendre une décision
def make_prediction_and_decision(symbol, interval="15m"):
    """
    Prédit le prix de clôture pour la prochaine période en utilisant le modèle RandomForestRegressor et prend une décision d'achat/vente.

    Arguments :
    - `symbol` (str) : Le symbole de la cryptomonnaie (par exemple 'BTC').
    - `interval` (str) : L'intervalle de temps utilisé pour la prédiction (par défaut '15m').

    Retourne :
    - Un dictionnaire contenant :
      - `symbol`: Le symbole de la cryptomonnaie.
      - `interval`: L'intervalle de temps utilisé.
      - `actual_time`: L'heure actuelle au format 'YYYY-MM-DD HH:MM:SS'.
      - `actual_price`: Le prix actuel récupéré depuis les données en temps réel.
      - `next_time`: L'heure du prochain intervalle.
      - `predicted_close_price`: Le prix de clôture prédit pour la prochaine période.
      - `decision`: La décision basée sur la prédiction ("Buy", "Sell" ou "Hold").
    """
    # Récupérer le prix actuel du flux en temps réel et l'heure du prochain intervalle
    stream_data = get_stream_price_and_next_time(symbol, interval)
    close_price_stream = stream_data["close_price"].iloc[0]
    next_time = stream_data["next_time"].iloc[0]

    # Récupérer les données historiques avec des lags
    id_interval    = get_id_interval(interval)
    id_symbol      = get_id_crypto_characteristics(symbol)
    feats, _ = get_historical_data(id_symbol, id_interval, lags=True)

    # Charger le modèle entraîné
    model = load_model(symbol, interval)

    # Prendre les dernières données pour faire la prédiction
    next_interval_data = feats.iloc[-1:]

    # Prédire le prix de clôture pour la prochaine période
    next_close_price = model.predict(next_interval_data)
    actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Prendre une décision d'investissement
    decision = make_decision(close_price_stream, next_close_price.item())

    return {
        "symbol": symbol,
        "interval": interval,
        "actual_time": actual_time,
        "actual_price": close_price_stream,
        "next_time": next_time,
        "predicted_close_price": round(next_close_price.item(), 2),
        "decision": decision
    }

# Fonction pour récupérer le prix de clôture du stream et le prochain intervalle de temps (par défaut 15 minutes)
def get_current_stream_price(symbol, interval='15m'):
    """
    Fonction pour récupérer le dernier prix de clôture de la cryptomonnaie en temps réel ainsi que le prochain intervalle de temps.

    Arguments :
    - `symbol` (str) : Le symbole de la cryptomonnaie (par exemple 'BTC').

    Description :
    - Récupère le dernier prix de clôture et calcule l'heure du prochain intervalle de temps basé sur le dernier `event_time`.
    - Utilise les données en temps réel stockées dans la table `stream_crypto_data`.

    Retour :
    - Un DataFrame Pandas avec le prix de clôture et le prochain intervalle de temps.
    """
    # try:
    #     connection = __connect_db()
    #     cursor = connection.cursor()

    #     # Requête SQL pour récupérer le prix de clôture et le prochain intervalle de temps
    #     query = '''
    #         SELECT 
    #             close_price,
    #             to_char(to_timestamp(event_time / 1000) + INTERVAL %s, 'YYYY-MM-DD HH24:MI:00') AS next_time
    #         FROM stream_crypto_data
    #         WHERE id_crypto_characteristics = (
    #             SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = %s
    #         )
    #         ORDER BY event_time DESC
    #         LIMIT 1
    #     '''
    #     # Conversion de l'intervalle en format PostgreSQL (par exemple '15 minutes')
    #     interval_mapping = {
    #         '15m': '15 minutes',
    #         '1h': '1 hour',
    #         '4h': '4 hours',
    #         '1d': '1 day',
    #         '1w': '1 week',
    #         '1M': '1 month'
    #     }
    #     pg_interval = interval_mapping.get(interval, '15 minutes')  # Utiliser 15 minutes par défaut

    #     # Exécuter la requête
    #     cursor.execute(query, (pg_interval, symbol))
    #     data = cursor.fetchall()
    #     connection.commit()

    #     # Retourner les données
    #     if data:
    #         return pd.DataFrame(data, columns=['close_price', 'next_time'])
    #     else:
    #         raise Exception(f"No stream data found for symbol {symbol}")

    # except Exception as e:
    #     print(f"get_stream_price_and_next_time: unable to retrieve data. Error: {e}")
    #     raise

    # finally:
    #     if connection is not None:
    #         connection.close()
