import logging
import os
from binance import ThreadedWebsocketManager
from scripts.lib_sql import insert_stream_crypto_data

# Configuration de l'API Binance
BINANCE_API_KEY    = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# Fonction pour gérer les messages reçus du WebSocket et les stocker dans la base de données
def handle_socket_message(msg):
    # print(msg)
    try:
        # Extraire les données de la bougie en temps réel
        stream_data = {
            'symbol': msg['s'],
            'event_time': msg['E'],
            'first_trade_id': msg['k']['f'],
            'last_trade_id': msg['k']['L'],
            'open_time': msg['k']['t'],
            'open_price': msg['k']['o'],
            'high_price': msg['k']['h'],
            'low_price': msg['k']['l'],
            'close_price': msg['k']['c'],
            'close_time': msg['k']['T'],
            'base_asset_volume': msg['k']['v'],
            'number_of_trades': msg['k']['n'],
            'is_this_kline_closed': msg['k']['x'],
            'quote_asset_volume': msg['k']['q'],
            'taker_buy_base_asset_volume': msg['k']['V'],
            'taker_buy_quote_asset_volume': msg['k']['Q']
        }

        # Stocker les données dans la base de données PostgreSQL
        insert_stream_crypto_data(stream_data)

    except Exception as e:
        logging.error(f"Erreur lors de la gestion des données du WebSocket : {e}")

# Fonction pour démarrer le flux WebSocket pour une paire donnée
def start_websocket_stream_from_binance(symbol, interval):
    try:
        logging.info(f"start_websocket_stream_from_binance : {symbol} : {interval}")

        twm = ThreadedWebsocketManager(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        # start is required to initialise its internal loop
        twm.start()

        # start kline socket
        twm.start_kline_socket(callback=handle_socket_message, symbol=symbol, interval=interval)
        twm.join()

    except Exception as e:
        logging.error(f"Erreur : {e}")

if __name__ == "__main__":
    start_websocket_stream_from_binance("BTCUSDT", "15m")
