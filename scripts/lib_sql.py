import os
import psycopg2
import pandas as pd

# Récupérer les caractéristiques de la cryptomonnaie
def get_id_crypto_characteristics(symbol):
    query = "SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = %s"
    return __get_query_to_one_value(query,(symbol,))

# Fonction pour récupérer l'id d'un intervale
def get_id_interval(interval):
    query = "SELECT id_interval FROM intervals WHERE intervals LIKE %s"
    return __get_query_to_one_value(query,(interval,))

# Fonction pour récupérer la dernière heure d'ouverture stockée
def get_last_open_time(id_symbol, id_interval):
    query = "SELECT MAX(open_time) FROM historical_crypto_data WHERE id_crypto_characteristics = %s AND id_interval = %s"
    return __get_query_to_one_value(query,(id_symbol,id_interval))

# Fonction pour récupérer les données historiques
def get_historical_data_to_df(id_symbol, id_interval):
    query = '''
        SELECT open_price, high_price, low_price, close_price,
            volume, quote_asset_volume, number_of_trades, 
            taker_buy_base_asset_volume, taker_buy_quote_asset_volume
        FROM historical_crypto_data
        WHERE id_crypto_characteristics = %s
          AND id_interval = %s
        ORDER BY open_time DESC
    '''
    return __get_query_to_df(query,(id_symbol,id_interval))

# Fonction pour stocker les données de CoinGecko
def insert_crypto_characteristics(crypto_data):
    try:
        connection = __connect_db()
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

        __execute_query_to_one_value(query, values)

    except Exception as e:
        print(f"Error inserting CoinGecko data: {e}")
        raise

# Fonction pour insérer des données historiques
def insert_historical_data( candlestick_data ):
    try:
        query = '''
            INSERT INTO historical_crypto_data (
                id_crypto_characteristics, id_interval, open_time, open_price, high_price, low_price, close_price, volume,
                close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_crypto_characteristics, id_interval, open_time) DO NOTHING
        '''

        __execute_query_to_many_values(query, candlestick_data)
    except Exception as e:
        print(f"Error inserting historical data: {e}")
        raise


############################################################
# Fonctions privés pour l'accés et les requêtes à la BDD
############################################################

# Connexion à la base de données PostgreSQL
def __connect_db():
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        connection = psycopg2.connect(DATABASE_URL)
        return connection

    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(f"Erreur lors de la connexion à la BDD : {error}")

# Query to DataFrame
def __get_query_to_df(query, values):
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        DATABASE_URL='postgresql://airflow:airflow@postgres/cryptoboat_db'

        df = pd.read_sql(query, DATABASE_URL, params=values)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(f"Erreur lors de la connexion à la BDD : {error}")

# Query to One value
def __get_query_to_one_value(query, where=None):
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.execute(query, where)
        data = cursor.fetchone()
        if data:
            return data[0]
        else:
            return None
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'execution de la requete : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Query to Many values
def __get_query_to_many_values(query, where=None):
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.execute(query, where)
        return cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'execution de la requete : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Execute to One values
def __execute_query_to_one_value(query, values):
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'execution de la requete : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Execute to Many values
def __execute_query_to_many_values(query, values):
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.executemany(query, values)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'execution de la requete : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()
