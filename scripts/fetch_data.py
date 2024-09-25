import requests
from binance.client import Client
from datetime import datetime
import pytz
import logging
from store_data import store_crypto_characteristics, store_historical_data

# Configuration de l'API Binance
BINANCE_API_KEY = 'your_api_key'
BINANCE_API_SECRET = 'your_api_secret'
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)

# URL de l'API CoinGecko
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/"

# Fonction pour convertir l'intervalle en millisecondes
def interval_to_milliseconds(interval):
    ms_per_unit = {
        "m": 60000,
        "h": 3600000,
        "d": 86400000,
        "w": 604800000
    }
    return int(interval[:-1]) * ms_per_unit[interval[-1]]

# Fonction pour récupérer les données de CoinGecko et les stocker
def fetch_coin_gecko_data(symbol):
    symbol_mapping = {
        "BTCUSDT": "bitcoin",
        "ETHUSDT": "ethereum",
    }
    
    coin_id = symbol_mapping.get(symbol)
    if not coin_id:
        logging.error(f"Aucun mapping CoinGecko pour le symbole {symbol}")
        return None

    try:
        url = f"{COINGECKO_URL}{coin_id}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Extraire les informations nécessaires
        name = data['name']
        market_cap = data['market_data']['market_cap'].get('usd', None)
        circulating_supply = data['market_data'].get('circulating_supply', None)
        max_supply = data['market_data'].get('max_supply', None)

        logging.info(f"Données CoinGecko récupérées pour {symbol}: Market Cap: {market_cap}, Circulating Supply: {circulating_supply}, Max Supply: {max_supply}")
        
        # Formatage des données pour le stockage
        formatted_data = {
            "name": name,
            "symbol": symbol,
            "market_cap": market_cap,
            "circulating_supply": circulating_supply,
            "max_supply": max_supply
        }

        # Stocker les données dans PostgreSQL
        store_crypto_characteristics(formatted_data)
        
        return formatted_data

    except requests.RequestException as e:
        logging.error(f"Erreur lors de la récupération des données CoinGecko: {e}")
        return None

# Fonction pour récupérer les données de Binance
def fetch_data_from_binance(symbol, interval, start_time, end_time):
    klines = []
    limit = 1000  # Limite maximale de données par requête

    while start_time < end_time:
        try:
            data = client.get_klines(
                symbol=symbol,
                interval=interval,
                startTime=start_time,
                endTime=min(end_time, start_time + limit * interval_to_milliseconds(interval))
            )
            if not data:
                break
            klines.extend(data)
            start_time = data[-1][0] + interval_to_milliseconds(interval)
            logging.info(f"Données récupérées jusqu'à : {datetime.fromtimestamp(start_time / 1000, tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            logging.error(f"Erreur lors de la récupération des données depuis Binance: {e}")
            raise

    return klines

# Fonction principale pour récupérer et stocker les données de Binance
def fetch_binance_data(symbol, interval="15m", start_date="2017-08-01", end_date="2024-08-01"):
    start_time = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC).timestamp() * 1000)
    end_time = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC).timestamp() * 1000)
    interval_id_mapping = {"15m": 1, "1h": 2, "4h": 3, "1d": 4, "1w": 5, "1M": 6}
    interval_id = interval_id_mapping.get(interval, 1)

    logging.info(f"Récupération des données historiques : symbole Binance: {symbol}, intervalle: {interval}")

    try:
        data = fetch_data_from_binance(symbol, interval, start_time, end_time)
    except Exception as e:
        logging.error(f"Erreur lors de la récupération des données depuis Binance: {e}")
        return

    # Formatage des données pour le stockage
    formatted_data = [
        (
            symbol,  # id_crypto_characteristics
            interval_id,  # id_interval
            k[0],  # open_time
            float(k[1]),  # open_price
            float(k[2]),  # high_price
            float(k[3]),  # low_price
            float(k[4]),  # close_price
            float(k[5]),  # volume
            k[6],  # close_time
            float(k[7]),  # quote_asset_volume
            float(k[8]),  # number_of_trades
            float(k[9]),  # taker_buy_base_asset_volume
            float(k[10])  # taker_buy_quote_asset_volume
        )
        for k in data
    ]

    # Stocker les données dans la base de données PostgreSQL
    try:
        store_historical_data(symbol, interval_id, formatted_data)
    except Exception as e:
        logging.error(f"Erreur lors du stockage des données: {e}")

# Fonction pour gérer le téléchargement et le stockage des données des deux API
def fetch_and_store_all_data(symbol, interval="15m", start_date="2017-08-01", end_date="2024-08-01"):
    fetch_coin_gecko_data(symbol)
    fetch_binance_data(symbol, interval, start_date, end_date)

if __name__ == '__main__':
    fetch_and_store_all_data("BTCUSDT")

