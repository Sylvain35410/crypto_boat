import logging
import os
from binance import ThreadedWebsocketManager
from scripts.lib_sql import insert_stream_crypto_data

# Configuration de l'API Binance
BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET = os.getenv('BINANCE_API_SECRET')

# Fonction pour gérer les messages reçus du WebSocket et les stocker dans la base de données
def handle_socket_message(msg):
    """
    Fonction pour gérer les messages reçus du flux WebSocket de Binance.

    Arguments :
    - msg (dict) : Le message reçu en temps réel du WebSocket contenant les données de trading.

    Description :
    - La fonction extrait les données de trading (données de bougie) du message reçu.
    - Elle formate les données dans un dictionnaire `stream_data` puis les insère dans la base de données PostgreSQL via la fonction `insert_stream_crypto_data`.

    Retour :
    - Aucun. Si les données sont correctement insérées, un message de succès est enregistré dans les logs. En cas d'erreur, un message d'erreur est enregistré.
    """
    try:
        # Extraire les données de la bougie en temps réel
        stream_data = {
            'symbol': msg['s'],                       # Symbole de la paire (ex : BTCUSDT)
            'event_time': msg['E'],                   # Heure de l'événement
            'first_trade_id': msg['k']['f'],           # ID du premier trade dans cette bougie
            'last_trade_id': msg['k']['L'],            # ID du dernier trade dans cette bougie
            'open_time': msg['k']['t'],                # Heure d'ouverture de la bougie
            'open_price': msg['k']['o'],               # Prix d'ouverture
            'high_price': msg['k']['h'],               # Prix le plus haut
            'low_price': msg['k']['l'],                # Prix le plus bas
            'close_price': msg['k']['c'],              # Prix de clôture
            'close_time': msg['k']['T'],               # Heure de clôture de la bougie
            'base_asset_volume': msg['k']['v'],        # Volume de l'actif échangé
            'number_of_trades': msg['k']['n'],         # Nombre de trades
            'is_this_kline_closed': msg['k']['x'],     # Indique si la bougie est fermée
            'quote_asset_volume': msg['k']['q'],       # Volume de l'actif de cotation échangé
            'taker_buy_base_asset_volume': msg['k']['V'], # Volume d'achat des takers (base asset)
            'taker_buy_quote_asset_volume': msg['k']['Q']  # Volume d'achat des takers (quote asset)
        }

        # Stocker les données dans la base de données PostgreSQL
        insert_stream_crypto_data(stream_data)
        logging.info(f"Real-time data for {stream_data['symbol']} successfully stored.")

    except Exception as e:
        logging.error(f"Error handling WebSocket data: {e}")

# Fonction pour démarrer le flux WebSocket pour une paire donnée
def start_websocket_stream_from_binance(symbol, interval):
    """
    Fonction pour démarrer un flux WebSocket Binance pour une paire spécifique et un intervalle de temps donné.

    Arguments :
    - symbol (str) : Le symbole de la paire de trading (par ex. 'BTCUSDT').
    - interval (str) : L'intervalle de temps pour les bougies (par ex. '1m', '15m', '1h').

    Description :
    - Cette fonction initialise et démarre un gestionnaire WebSocket (`ThreadedWebsocketManager`) pour recevoir les bougies de trading en temps réel.
    - Les messages reçus sont traités par la fonction `handle_socket_message` pour être stockés dans la base de données.
    
    Retour :
    - Aucun. La fonction continue de recevoir des messages WebSocket jusqu'à l'arrêt manuel. En cas d'erreur, un message d'erreur est enregistré.
    """
    try:
        # Démarrage du flux WebSocket pour la paire spécifiée avec l'intervalle donné
        logging.info(f"Starting WebSocket stream for {symbol} with interval {interval}.")

        # Initialiser le gestionnaire WebSocket avec les clés API
        twm = ThreadedWebsocketManager(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        
        # Initialiser la boucle interne du WebSocketManager
        twm.start()

        # Démarrer le flux WebSocket pour les bougies (kline)
        twm.start_kline_socket(callback=handle_socket_message, symbol=symbol, interval=interval)
        twm.join()  # Le WebSocketManager reste actif pour recevoir les messages

    except Exception as e:
        logging.error(f"Error: {e}")
