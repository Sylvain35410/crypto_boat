from datetime import datetime
import logging
import os
import pytz
import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException
from scripts.lib_sql import get_id_crypto_characteristics, get_last_open_time, get_id_interval, insert_historical_data, insert_crypto_characteristics
import psycopg2

# Configuration de l'API Binance
BINANCE_API_KEY    = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# URL de l'API CoinGecko
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/"

# Fonction pour convertir un intervalle en millisecondes
def interval_to_milliseconds(interval):
    """
    Convertit un intervalle donné (comme '15m', '1h') en millisecondes.

    Arguments :
    - interval (str) : L'intervalle à convertir (par ex. '15m' pour 15 minutes).

    Retour :
    - (int) : L'équivalent de cet intervalle en millisecondes.
    """
    ms_per_unit = {
        "m": 60000,
        "h": 3600000,
        "d": 86400000,
        "w": 604800000
    }
    return int(interval[:-1]) * ms_per_unit[interval[-1]]

# Fonction pour récupérer et stocker les données depuis CoinGecko
def fetch_coin_gecko_data(symbol):
    """
    Récupère les données de CoinGecko et les stocke dans la base de données PostgreSQL.

    Arguments :
    - symbol (str) : Le symbole de la cryptomonnaie (par ex. 'BTCUSDT').

    Description :
    - Cette fonction utilise l'API CoinGecko pour récupérer des informations sur une cryptomonnaie spécifique et les stocke dans la base de données.
    """
    symbol_mapping = {
        "BTCUSDT": "bitcoin",
        "ETHUSDT": "ethereum",
    }
    
    coin_id = symbol_mapping.get(symbol)
    if not coin_id:
        logging.error(f"No CoinGecko mapping for symbol {symbol}")
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

        logging.info(f"CoinGecko data fetched for {symbol}: Market Cap: {market_cap}, Circulating Supply: {circulating_supply}, Max Supply: {max_supply}")
        
        # Formatage des données pour le stockage
        formatted_data = {
            "name": name,
            "symbol": symbol,
            "market_cap": market_cap,
            "circulating_supply": circulating_supply,
            "max_supply": max_supply
        }

    except requests.RequestException as e:
        raise Exception(f"Error fetching CoinGecko data: {e}")

    try:
        # Stocker les données dans PostgreSQL
        insert_crypto_characteristics(formatted_data)
        logging.info(f"CoinGecko data for {symbol} stored successfully.")
    except Exception as error:
        raise

# Fonction pour récupérer les données depuis Binance
def fetch_data_from_binance(symbol, interval, start_time, end_time):
    """
    Récupère les données historiques de Binance pour un symbole donné et un intervalle de temps.

    Arguments :
    - symbol (str) : Le symbole de la cryptomonnaie (par ex. 'BTCUSDT').
    - interval (str) : L'intervalle de temps des données (par ex. '15m').
    - start_time (int) : L'heure de début en millisecondes (timestamp).
    - end_time (int) : L'heure de fin en millisecondes (timestamp).

    Retour :
    - (list) : Liste des données Kline récupérées de Binance.
    """
    klines = []
    limit = 1000  # Limite maximale de données par requête

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    # Vérification de la connexion API
    api_ready, msg = __test_api_key(client, BinanceAPIException)
    if api_ready is not True:
        raise Exception(msg)

    while start_time < end_time:
        try:
            data = client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
                startTime=start_time,
                endTime=min(end_time, start_time + limit * interval_to_milliseconds(interval))
            )
            if not data:
                break
            klines.extend(data)
            start_time = data[-1][0] + interval_to_milliseconds(interval)
            logging.info(f"Data fetched up to: {datetime.fromtimestamp(start_time / 1000, tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            raise Exception(f"Error fetching data from Binance: {e}")

    return klines

# Fonction principale pour récupérer et stocker les données Binance
def fetch_binance_data(symbol, interval, start_date, end_date):
    """
    Récupère et stocke les données historiques de Binance pour un symbole donné, sur une période donnée.

    Arguments :
    - symbol (str) : Le symbole de la cryptomonnaie (par ex. 'BTCUSDT').
    - interval (str) : L'intervalle de temps des données (par ex. '15m').
    - start_date (str) : La date de début au format 'YYYY-MM-DD'.
    - end_date (str) : La date de fin au format 'YYYY-MM-DD'.

    Description :
    - Cette fonction récupère les données de Binance et les insère dans la base de données PostgreSQL.
    """
    start_time = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC).timestamp() * 1000)
    end_time = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC).timestamp() * 1000)

    id_interval    = get_id_interval(interval)
    id_symbol      = get_id_crypto_characteristics(symbol)
    last_open_time = get_last_open_time(id_symbol, id_interval)

    # Aucune donnée pour ce symbole
    if not last_open_time:
        last_open_time = start_time

    # Les dates demandées sont déjà récupérées
    if last_open_time > end_time:
        logging.info(f"Data already fetched: Binance symbol: {symbol}, interval: {interval}")
        return

    # Une partie des données est déjà récupérée
    if last_open_time > start_time:
        start_time = last_open_time

    try:
        logging.info(f"Fetching historical data: Binance symbol: {symbol}, interval: {interval}")
        data = fetch_data_from_binance(symbol, interval, start_time, end_time)
    except Exception as e:
        raise

    # Formatage des données pour le stockage
    formatted_data = [
        (
            id_symbol,    # id_crypto_characteristics
            id_interval,  # id_interval
            k[0],         # open_time
            float(k[1]),  # open_price
            float(k[2]),  # high_price
            float(k[3]),  # low_price
            float(k[4]),  # close_price
            float(k[5]),  # volume
            k[6],         # close_time
            float(k[7]),  # quote_asset_volume
            float(k[8]),  # number_of_trades
            float(k[9]),  # taker_buy_base_asset_volume
            float(k[10])  # taker_buy_quote_asset_volume
        )
        for k in data
    ]

    # Stocker les données dans la base de données PostgreSQL
    if formatted_data:
        try:
            insert_historical_data(formatted_data)
            logging.info(f"Binance historical data for {symbol} stored successfully.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error storing Binance historical data in PostgreSQL: {error}")
            raise
        except Exception as e:
            raise

# Fonction pour tester la validité des clés API
def __test_api_key(client, BinanceAPIException):
    """
    Vérifie si les clés API Binance fournies sont valides.

    Arguments :
    - client : Le client Binance utilisé pour effectuer les requêtes API.
    - BinanceAPIException : L'exception spécifique levée par l'API Binance en cas de problème.

    Retour :
    - (bool, str) : True si les clés API sont valides, sinon False avec un message d'erreur.
    """
    try:
        client.get_account()
        return True, "API key validated successfully"
    
    except BinanceAPIException as e:   
        if e.code in [-2015, -2014]:
            msg = f"Your API key is either incorrect or IP blocked. Check your key formatting or IP settings."
        elif e.code == -2021:
            msg = f"Timestamp for this request was ahead of server's time. Ensure your OS time is synced."
        elif e.code == -1022:
            msg = f"Signature for this request is not valid"
        elif e.code == -1021:
            msg = "Your operating system time is not properly synced. Sync ntp time with 'pool.ntp.org'"
        else:
            msg = f"Unexpected API Error: {str(e)}"
        return False, msg
    except Exception as e:
        return False, f"Fallback exception occurred: {e}"
