from datetime import datetime
import logging
import os
import pytz
import requests
from binance.client import Client
from binance.exceptions import BinanceAPIException
from scripts.lib_sql import get_crypto_characteristics, get_last_open_time, get_id_interval, insert_historical_data, insert_crypto_characteristics

# Configuration de l'API Binance
BINANCE_API_KEY    = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

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

    except requests.RequestException as e:
        raise Exception(f"Erreur lors de la récupération des données CoinGecko: {e}")

    try:
        # Stocker les données dans PostgreSQL
        insert_crypto_characteristics(formatted_data)
        logging.info(f"Données CoinGecko pour {symbol} stockées avec succès.")
    except Exception as error:
        raise

# Fonction pour récupérer les données de Binance
def fetch_data_from_binance(symbol, interval, start_time, end_time):
    klines = []
    limit = 1000  # Limite maximale de données par requête

    client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
    # Vérification de la connexion
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
            logging.info(f"Données récupérées jusqu'à : {datetime.fromtimestamp(start_time / 1000, tz=pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            raise Exception(f"Erreur lors de la récupération des données depuis Binance: {e}")

    return klines

# Fonction principale pour récupérer et stocker les données de Binance
def fetch_binance_data(symbol, interval, start_date, end_date):
    start_time = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC).timestamp() * 1000)
    end_time = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC).timestamp() * 1000)

    id_interval    = get_id_interval( interval )
    id_symbol      = get_crypto_characteristics(symbol)
    last_open_time = get_last_open_time(id_symbol, id_interval)

    # Aucune donnée pour ce symbole
    if not last_open_time:
        last_open_time = start_time

    # Les dates demandées sont déjà récupérées
    if last_open_time > end_time:
        logging.info(f"Les données sont déjà récupérées : symbole Binance: {symbol}, intervalle: {interval}")
        return

    # Une partie des données est déjà récupérée
    if last_open_time > start_time:
        start_time = last_open_time

    try:
        logging.info(f"Récupération des données historiques : symbole Binance: {symbol}, intervalle: {interval}")
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
            insert_historical_data( formatted_data )

            logging.info(f"Données historiques de Binance pour {symbol} stockées avec succès.")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Erreur lors du stockage des données historiques de Binance dans PostgreSQL : {error}")
            raise
        except Exception as e:
            raise

def __test_api_key(client, BinanceAPIException):
    """Checks to see if API keys supplied returns errors

    Args:
        client (class): binance client class
        BinanceAPIException (clas): binance exeptions class

    Returns:
        bool | msg: true/false depending on success, and message
    """
    try:
        client.get_account()
        return True, "API key validated succesfully"
    
    except BinanceAPIException as e:   
        if e.code in  [-2015,-2014]:
            bad_key = "Your API key is not formatted correctly..."
            america = "If you are in america, you will have to update the config to set AMERICAN_USER: True"
            ip_b = "If you set an IP block on your keys make sure this IP address is allowed. check ipinfo.io/ip"
            msg = f"Your API key is either incorrect, IP blocked, or incorrect tld/permissons...\n  most likely: {bad_key}\n  {america}\n  {ip_b}"
        elif e.code == -2021:
            issue = "https://github.com/CyberPunkMetalHead/Binance-volatility-trading-bot/issues/28"
            desc = "Ensure your OS is time synced with a timeserver. See issue."
            msg = f"Timestamp for this request was 1000ms ahead of the server's time.\n  {issue}\n  {desc}"
        elif e.code == -1022:
            msg = f"Signature for this request is not valid"
        elif e.code == -1021:
            desc = "Your operating system time is not properly synced... Please sync ntp time with 'pool.ntp.org'"
            msg = f"{desc}\nmaybe try this:\n\tsudo ntpdate pool.ntp.org"
        else:
            msg = "Encountered an API Error code that was not caught nicely, please open issue...\n"
            msg += str(e)

        return False, msg
    except Exception as e:
        return False, f"Fallback exception occured:\n{e}"
