import psycopg2
import logging
from scripts.lib_sql import __connect_db

# Fonction pour stocker les données en temps réel de Binance dans PostgreSQL (stream_crypto_data)
def store_stream_data(stream_data):
    """
    Fonction pour stocker les données de flux en temps réel de Binance dans la base de données PostgreSQL.

    Arguments :
    - stream_data (dict) : Dictionnaire contenant les données de flux (stream) en temps réel récupérées de Binance.

    Description :
    - La fonction établit une connexion à la base de données PostgreSQL.
    - Elle insère les données de flux en temps réel dans la table `stream_crypto_data`.
    - Si un conflit survient (par ex., si une ligne existe déjà avec le même id_crypto_characteristics), les données sont mises à jour avec les nouvelles valeurs.

    Exemple de `stream_data` :
    {
        'symbol': 'BTCUSDT',
        'event_time': 1632873600000,
        'first_trade_id': 100,
        'last_trade_id': 200,
        'open_time': 1632873600000,
        'open_price': 43000.5,
        'high_price': 43100.0,
        'low_price': 42950.0,
        'close_price': 43050.0,
        'close_time': 1632877200000,
        'base_asset_volume': 100.0,
        'number_of_trades': 150,
        'is_this_kline_closed': True,
        'quote_asset_volume': 4305000.0,
        'taker_buy_base_asset_volume': 50.0,
        'taker_buy_quote_asset_volume': 2152500.0
    }

    Retour :
    - Aucun. Si l'insertion ou la mise à jour échoue, une exception est levée.
    """
    try:
        # Connexion à la base de données
        connection = __connect_db()
        cursor = connection.cursor()

        # Requête SQL pour insérer les données en temps réel
        query = '''
            INSERT INTO stream_crypto_data (
                id_crypto_characteristics, event_time, first_trade_id, last_trade_id, open_time, open_price, high_price, 
                low_price, close_price, close_time, base_asset_volume, number_of_trades, is_this_kline_closed, 
                quote_asset_volume, taker_buy_base_asset_volume, taker_buy_quote_asset_volume
            )
            VALUES (
                (SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = %s), %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id_crypto_characteristics) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                first_trade_id = EXCLUDED.first_trade_id,
                last_trade_id = EXCLUDED.last_trade_id,
                open_time = EXCLUDED.open_time,
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                close_time = EXCLUDED.close_time,
                base_asset_volume = EXCLUDED.base_asset_volume,
                number_of_trades = EXCLUDED.number_of_trades,
                is_this_kline_closed = EXCLUDED.is_this_kline_closed,
                quote_asset_volume = EXCLUDED.quote_asset_volume,
                taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
                taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume
        '''

        # Valeurs à insérer dans la base de données
        values = (
            stream_data['symbol'], stream_data['event_time'], stream_data['first_trade_id'], stream_data['last_trade_id'],
            stream_data['open_time'], stream_data['open_price'], stream_data['high_price'], stream_data['low_price'],
            stream_data['close_price'], stream_data['close_time'], stream_data['base_asset_volume'], stream_data['number_of_trades'],
            stream_data['is_this_kline_closed'], stream_data['quote_asset_volume'], stream_data['taker_buy_base_asset_volume'],
            stream_data['taker_buy_quote_asset_volume']
        )

        # Exécution de la requête SQL
        cursor.execute(query, values)
        connection.commit()
        cursor.close()

        logging.info(f"Données de flux en temps réel pour {stream_data['symbol']} stockées avec succès.")
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(f"Erreur lors du stockage des données de flux en temps réel dans PostgreSQL : {error}")
    finally:
        # Fermeture de la connexion à la base de données
        if connection is not None:
            connection.close()
