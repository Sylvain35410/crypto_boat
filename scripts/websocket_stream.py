from binance.client import Client
from binance.websockets import BinanceSocketManager
import logging
from store_data import store_stream_data

# Configuration de l'API Binance
BINANCE_API_KEY = 'your_api_key'
BINANCE_API_SECRET = 'your_api_secret'
client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
bm = BinanceSocketManager(client)

# Fonction pour gérer les messages reçus du WebSocket et les stocker dans la base de données
def handle_socket_message(msg):
    try:
        # Extraire les données de la bougie en temps réel
        stream_data = {
            'symbol': msg['s'],
            'event_time': msg['E'],
            'first_trade_id': msg['f'],
            'last_trade_id': msg['L'],
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
        store_stream_data(stream_data)
        logging.info(f"Données du flux WebSocket pour {msg['s']} stockées avec succès.")

    except Exception as e:
        logging.error(f"Erreur lors de la gestion des données du WebSocket : {e}")

# Fonction pour démarrer le flux WebSocket pour une paire donnée
def stream_data_from_binance(symbol, interval='1m'):
    conn_key = bm.start_kline_socket(symbol, handle_socket_message, interval=interval)
    bm.start()

# Fonction principale pour démarrer les flux pour BTCUSDT et ETHUSDT
def start_websocket_streams():
    logging.info("Démarrage des flux WebSocket pour BTCUSDT et ETHUSDT.")
    stream_data_from_binance("BTCUSDT")
    stream_data_from_binance("ETHUSDT")

if __name__ == "__main__":
    start_websocket_streams()

