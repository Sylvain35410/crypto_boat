

import psycopg2
import logging

# Fonction pour stocker les données de CoinGecko dans PostgreSQL
def store_crypto_characteristics(crypto_data):
    connection = None
    try:
        connection = psycopg2.connect(
            dbname="crypto_data",
            user="airflow",  
            password="airflow",  
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()

        query = '''
            INSERT INTO crypto_characteristics (name, symbol, market_cap, circulating_supply, max_supply)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (symbol) DO UPDATE 
            SET market_cap = EXCLUDED.market_cap,
                circulating_supply = EXCLUDED.circulating_supply,
                max_supply = EXCLUDED.max_supply
        '''
        values = (
            crypto_data['name'], 
            crypto_data['symbol'], 
            crypto_data['market_cap'], 
            crypto_data['circulating_supply'], 
            crypto_data['max_supply']
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()

        logging.info(f"Données CoinGecko pour {crypto_data['symbol']} stockées avec succès.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erreur lors de l'insertion des données CoinGecko dans PostgreSQL : {error}")
    finally:
        if connection is not None:
            connection.close()

# Fonction pour stocker les données historiques de Binance dans PostgreSQL
def store_historical_data(symbol, interval_id, candlestick_data):
    connection = None
    try:
        connection = psycopg2.connect(
            dbname="crypto_data",
            user="airflow",
            password="airflow",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()

        query = '''
            INSERT INTO historical_crypto_data (
                id_crypto_characteristics, id_interval, open_time, open_price, high_price, low_price, close_price, 
                volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, 
                taker_buy_quote_asset_volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        cursor.executemany(query, candlestick_data)
        connection.commit()
        cursor.close()

        logging.info(f"Données historiques de Binance pour {symbol} stockées avec succès.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erreur lors du stockage des données historiques de Binance dans PostgreSQL : {error}")
    finally:
        if connection is not None:
            connection.close()

# Fonction pour stocker les données en temps réel de Binance dans PostgreSQL (stream_crypto_data)
def store_stream_data(stream_data):
    connection = None
    try:
        connection = psycopg2.connect(
            dbname="crypto_data",
            user="airflow",
            password="airflow",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()

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

        values = (
            stream_data['symbol'], stream_data['event_time'], stream_data['first_trade_id'], stream_data['last_trade_id'],
            stream_data['open_time'], stream_data['open_price'], stream_data['high_price'], stream_data['low_price'],
            stream_data['close_price'], stream_data['close_time'], stream_data['base_asset_volume'], stream_data['number_of_trades'],
            stream_data['is_this_kline_closed'], stream_data['quote_asset_volume'], stream_data['taker_buy_base_asset_volume'],
            stream_data['taker_buy_quote_asset_volume']
        )
        cursor.execute(query, values)
        connection.commit()
        cursor.close()

        logging.info(f"Données de flux en temps réel pour {stream_data['symbol']} stockées avec succès.")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Erreur lors du stockage des données de flux en temps réel dans PostgreSQL : {error}")
    finally:
        if connection is not None:
            connection.close()

