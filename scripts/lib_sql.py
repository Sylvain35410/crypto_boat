import os
import psycopg2
import pandas as pd
import hashlib

# Fonctions privées pour l'accès et les requêtes à la BDD : 

# Connexion à la base de données PostgreSQL
def __connect_db():
    """
    Fonction pour établir une connexion à la base de données PostgreSQL.

    Arguments : Aucun

    Description :
    - Récupère l'URL de la base de données à partir de la variable d'environnement `DATABASE_URL`.
    - Tente d'établir une connexion à la base de données via `psycopg2.connect()`.
    - Si la connexion échoue, une exception est levée avec un message d'erreur détaillé.

    Retour :
    - Renvoie l'objet `connection` qui représente la connexion à la base de données.
    """
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        connection = psycopg2.connect(DATABASE_URL)
        return connection

    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(f"Erreur lors de la connexion à la BDD : {error}")

# Vérifier la santé de la base de données
def check_database_health():
    """
    Fonction pour vérifier si la base de données est accessible.

    Retour :
    - Retourne True si la base de données est connectée correctement, False sinon.
    """
    try:
        conn = __connect_db()
        conn.close()
        return True
    except Exception as e:
        print(f"Database connection error: {e}")
        return False

# Hacher le mot de passe pour le stockage
def hash_password(password):
    """
    Fonction pour hacher un mot de passe avant de le stocker dans la base de données.

    Arguments :
    - `password` (str) : Le mot de passe en clair à hacher.

    Retour :
    - Le mot de passe haché sous forme de chaîne de caractères.
    """
    return hashlib.sha256(password.encode()).hexdigest()

# Ajouter un utilisateur dans la base de données PostgreSQL
def add_user_to_db(username, email, password):
    """
    Fonction pour ajouter un nouvel utilisateur dans la table `users`.

    Arguments :
    - `username` (str) : Le nom d'utilisateur.
    - `email` (str) : L'email de l'utilisateur.
    - `password` (str) : Le mot de passe en clair à hacher et stocker.
    """
    try:
        hashed_password = hash_password(password)
        conn = __connect_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO users (username, email, password_hash, created_at, updated_at) VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            (username, email, hashed_password)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Error adding user: {e}")

# Supprimer un utilisateur de la base de données PostgreSQL
def delete_user_from_db(username):
    """
    Fonction pour supprimer un utilisateur de la table `users` à partir de son nom d'utilisateur.

    Arguments :
    - `username` (str) : Le nom d'utilisateur à supprimer.
    """
    try:
        conn = __connect_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE username = %s", (username,))
        conn.commit()
        conn.close()
    except Exception as e:
        raise RuntimeError(f"Error deleting user: {e}")

# Fonction pour obtenir l'utilisateur courant avec authentification basique
def get_current_user(credentials):
    """
    Fonction pour récupérer les informations de l'utilisateur courant.

    Arguments :
    - `credentials` : Les informations d'authentification (nom d'utilisateur et mot de passe).

    Retour :
    - Le nom d'utilisateur si les informations d'authentification sont correctes, sinon une exception est levée.
    """
    try:
        conn = __connect_db()
        cursor = conn.cursor()
        cursor.execute("SELECT username, password_hash FROM users WHERE username = %s", (credentials.username,))
        user = cursor.fetchone()
        conn.close()

        if user and user[1] == hash_password(credentials.password):
            return credentials.username
        raise Exception("Invalid credentials")
    except Exception as e:
        raise RuntimeError(f"Authentication error: {e}")

# Query to DataFrame
def __get_query_to_df(query, values):
    """
    Fonction pour exécuter une requête SQL et retourner les résultats sous forme de DataFrame Pandas.

    Arguments :
    - `query` (str) : La requête SQL à exécuter.
    - `values` (tuple) : Les paramètres à utiliser dans la requête SQL (par exemple, des valeurs pour les clauses WHERE).

    Description :
    - La fonction utilise `pd.read_sql()` pour exécuter la requête SQL et obtenir les résultats directement sous forme de DataFrame.
    - Elle se connecte à la base de données, exécute la requête et retourne les résultats.

    Retour :
    - Un DataFrame Pandas contenant les résultats de la requête SQL.
    """
    try:
        DATABASE_URL = os.getenv('DATABASE_URL')
        DATABASE_URL = 'postgresql://airflow:airflow@postgres/cryptoboat_db'

        df = pd.read_sql(query, DATABASE_URL, params=values)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        raise Exception(f"Erreur lors de la connexion à la BDD : {error}")

# Query to One value
def __get_query_to_one_value(query, where=None):
    """
    Fonction pour exécuter une requête SQL et retourner une seule valeur.

    Arguments :
    - `query` (str) : La requête SQL à exécuter.
    - `where` (tuple) : Les paramètres pour la requête SQL (facultatif).

    Description :
    - La fonction exécute une requête SQL et récupère le premier résultat (une seule valeur).
    - Si aucun résultat n'est trouvé, elle retourne `None`.

    Retour :
    - La première valeur de la requête SQL ou `None` si aucun résultat n'est trouvé.
    """
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
        print(f"Erreur lors de l'exécution de la requête : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Query to Many values
def __get_query_to_many_values(query, where=None):
    """
    Fonction pour exécuter une requête SQL et retourner plusieurs valeurs (liste de tuples).

    Arguments :
    - `query` (str) : La requête SQL à exécuter.
    - `where` (tuple) : Les paramètres pour la requête SQL (facultatif).

    Description :
    - La fonction exécute une requête SQL et récupère plusieurs résultats (plusieurs lignes sous forme de tuples).

    Retour :
    - Une liste de tuples contenant les résultats de la requête SQL.
    """
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.execute(query, where)
        return cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'exécution de la requête : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Récupérer les caractéristiques de la cryptomonnaie
def get_id_crypto_characteristics(symbol):
    """
    Fonction pour récupérer l'ID d'une cryptomonnaie à partir de son symbole.

    Arguments :
    - `symbol` (str) : Le symbole de la cryptomonnaie (par exemple 'BTC' pour Bitcoin).

    Description :
    - Exécute une requête SQL pour récupérer l'ID de la cryptomonnaie correspondant au symbole donné.

    Retour :
    - L'ID de la cryptomonnaie ou `None` si le symbole n'est pas trouvé.
    """
    query = "SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = %s"
    return __get_query_to_one_value(query, (symbol,))

# Fonction pour récupérer l'id d'un intervalle
def get_id_interval(interval):
    """
    Fonction pour récupérer l'ID d'un intervalle de temps à partir de sa valeur (par exemple '15m', '1d').

    Arguments :
    - `interval` (str) : L'intervalle de temps (par exemple '15m' pour 15 minutes).

    Description :
    - Exécute une requête SQL pour récupérer l'ID de l'intervalle correspondant à l'intervalle donné.

    Retour :
    - L'ID de l'intervalle ou `None` si l'intervalle n'est pas trouvé.
    """
    query = "SELECT id_interval FROM intervals WHERE intervals LIKE %s"
    return __get_query_to_one_value(query, (interval,))

# Fonction pour récupérer la dernière heure d'ouverture stockée
def get_last_open_time(id_symbol, id_interval):
    """
    Fonction pour récupérer le temps d'ouverture maximum enregistré pour une cryptomonnaie spécifique et un intervalle donné.

    Arguments :
    - `id_symbol` (int) : L'ID de la cryptomonnaie.
    - `id_interval` (int) : L'ID de l'intervalle de temps.

    Description :
    - Exécute une requête SQL pour récupérer le dernier temps d'ouverture enregistré pour la cryptomonnaie et l'intervalle donnés.

    Retour :
    - Le temps d'ouverture maximum (en timestamp) ou `None` si aucune donnée n'est trouvée.
    """
    query = "SELECT MAX(open_time) FROM historical_crypto_data WHERE id_crypto_characteristics = %s AND id_interval = %s"
    return __get_query_to_one_value(query, (id_symbol, id_interval))

# Fonction fusionnée pour récupérer les données historiques
def get_historical_data(id_symbol, id_interval, start_time=None, end_time=None, lags=False, limit=300000):
    """
    Fonction pour récupérer les données historiques de la cryptomonnaie avec ou sans décalages (lags).

    Arguments :
    - `id_symbol` (int) : L'ID de la cryptomonnaie.
    - `id_interval` (int) : L'ID de l'intervalle de temps.
    - `start_time` (int) : Le timestamp de début de la période (facultatif).
    - `end_time` (int) : Le timestamp de fin de la période (facultatif).
    - `lags` (bool) : Si True, récupère les données avec des colonnes de décalage (lags), sinon sans.
    - `limit` (int) : Limite du nombre de lignes à récupérer (par défaut à 300000).

    Description :
    - Si `lags` est `True`, les données récupérées incluent des colonnes supplémentaires avec des décalages.
    - Si `lags` est `False`, les données récupérées sont simples (sans décalage).
    - La fonction renvoie soit un DataFrame avec les données soit un tuple de features/target si des lags sont inclus.

    Retour :
    - Si `lags` est False : retourne un DataFrame Pandas avec les données historiques.
    - Si `lags` est True : retourne un tuple `(feats, target)` avec les features et la cible pour les prédictions.
    """
    if lags:
        try:
            connection = __connect_db()
            cursor = connection.cursor()

            # Exécution de la requête pour récupérer les données historiques
            query = '''
                SELECT * 
                FROM historical_crypto_data 
                WHERE id_crypto_characteristics = %s
                AND id_interval = %s
                ORDER BY open_time ASC
                LIMIT %s
            '''
            cursor.execute(query, (id_symbol, id_interval, limit))
            data_raw = cursor.fetchall()
            connection.commit()

            # Transformation des données en DataFrame
            data = pd.DataFrame(data_raw, columns=[
                'id', 'id_crypto_characteristics', 'id_interval', 'open_time', 'open_price', 'high_price',
                'low_price', 'close_price', 'volume', 'close_time', 'quote_asset_volume', 'number_of_trades',
                'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
            ])

            # Définition des colonnes à décaler
            lag_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume',
                           'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
                           'taker_buy_quote_asset_volume']
            lag_count = 2  # Nombre de décalages

            # Ajout des colonnes de décalage
            for col in lag_columns:
                for lag in range(1, lag_count + 1):
                    data[f'{col}_lag-{lag}'] = data[col].shift(lag)

            # Ajout de la colonne de prix de clôture suivant
            data['next_close_price'] = data['close_price'].shift(-1)

            # Suppression des lignes avec des valeurs manquantes
            data = data.dropna()

            # Séparation des caractéristiques et de la cible
            feats = data.drop(['open_time', 'close_time', 'next_close_price', 'id_crypto_characteristics', 'id_interval'], axis=1)
            target = data['next_close_price']

            return feats, target

        except Exception as e:
            print(f"get_historical_data (with lag): unable to retrieve data. Error: {str(e)}")
            raise

        finally:
            if connection is not None:
                connection.close()

    else:
        # Si lags est False, on récupère les données sans décalage
        query = '''
            SELECT open_price, high_price, low_price, close_price,
                volume, quote_asset_volume, number_of_trades, 
                taker_buy_base_asset_volume, taker_buy_quote_asset_volume
            FROM historical_crypto_data
            WHERE id_crypto_characteristics = %s
              AND id_interval = %s
              AND open_time BETWEEN %s AND %s
            ORDER BY open_time DESC
        '''
        return __get_query_to_df(query, (id_symbol, id_interval, start_time, end_time))

# Fonction pour récupérer le prix de clôture du stream et le prochain intervalle de temps (par défaut 15 minutes)
def get_stream_price_and_next_time(symbol, interval='15m'):
    """
    Fonction pour récupérer le dernier prix de clôture de la cryptomonnaie en temps réel ainsi que le prochain intervalle de temps.

    Arguments :
    - `symbol` (str) : Le symbole de la cryptomonnaie (par exemple 'BTC').
    - `interval` (str) : L'intervalle de temps (par défaut '15m').

    Description :
    - Récupère le dernier prix de clôture et calcule l'heure du prochain intervalle de temps basé sur le dernier `event_time`.
    - Utilise les données en temps réel stockées dans la table `stream_crypto_data`.

    Retour :
    - Un DataFrame Pandas avec le prix de clôture et le prochain intervalle de temps.
    """
    try:
        connection = __connect_db()
        cursor = connection.cursor()

        # Requête SQL pour récupérer le prix de clôture et le prochain intervalle de temps
        query = '''
            SELECT 
                close_price,
                to_char(to_timestamp(event_time / 1000) + INTERVAL %s, 'YYYY-MM-DD HH24:MI:00') AS next_time
            FROM stream_crypto_data
            WHERE id_crypto_characteristics = (
                SELECT id_crypto_characteristics FROM crypto_characteristics WHERE symbol = %s
            )
            ORDER BY event_time DESC
            LIMIT 1
        '''
        # Conversion de l'intervalle en format PostgreSQL (par exemple '15 minutes')
        interval_mapping = {
            '15m': '15 minutes',
            '1h': '1 hour',
            '4h': '4 hours',
            '1d': '1 day',
            '1w': '1 week',
            '1M': '1 month'
        }
        pg_interval = interval_mapping.get(interval, '15 minutes')  # Utiliser 15 minutes par défaut

        # Exécuter la requête
        cursor.execute(query, (pg_interval, symbol))
        data = cursor.fetchall()
        connection.commit()

        # Retourner les données
        if data:
            return pd.DataFrame(data, columns=['close_price', 'next_time'])
        else:
            raise Exception(f"No stream data found for symbol {symbol}")

    except Exception as e:
        print(f"get_stream_price_and_next_time: unable to retrieve data. Error: {e}")
        raise

    finally:
        if connection is not None:
            connection.close()

# Execute to One value
def __execute_query_to_one_value(query, values):
    """
    Fonction pour exécuter une requête SQL d'insertion ou de mise à jour qui affecte une seule valeur.

    Arguments :
    - `query` (str) : La requête SQL à exécuter (par exemple INSERT, UPDATE).
    - `values` (tuple) : Les paramètres à insérer dans la requête SQL.

    Description :
    - La fonction exécute une requête SQL qui n'a pas besoin de retourner des valeurs (comme INSERT ou UPDATE).
    - Elle se connecte à la base de données, exécute la requête et effectue un commit.

    Retour :
    - Aucun.
    """
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.execute(query, values)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'exécution de la requête : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Execute to Many values
def __execute_query_to_many_values(query, values):
    """
    Fonction pour exécuter une requête SQL qui affecte plusieurs lignes (INSERT ou UPDATE multiple).

    Arguments :
    - `query` (str) : La requête SQL à exécuter.
    - `values` (list of tuples) : Les données à insérer ou mettre à jour.

    Description :
    - Cette fonction exécute des requêtes SQL pour affecter plusieurs valeurs à la fois (par exemple, plusieurs INSERTS).
    - Utilise `executemany()` pour traiter efficacement un grand nombre de données.

    Retour :
    - Aucun.
    """
    try:
        connection = __connect_db()
        cursor = connection.cursor()
        cursor.executemany(query, values)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'exécution de la requête : {query}")
        raise
    finally:
        if connection is not None:
            connection.close()

# Fonction pour stocker les données de CoinGecko
def insert_crypto_characteristics(crypto_data):
    """
    Fonction pour insérer les caractéristiques d'une cryptomonnaie dans la base de données.

    Arguments :
    - `crypto_data` (dict) : Un dictionnaire contenant les informations de la cryptomonnaie (nom, symbole, market_cap, etc.).

    Description :
    - La fonction exécute une requête SQL pour insérer ou mettre à jour les données de la cryptomonnaie dans la base de données.

    Retour :
    - Aucun.
    """
    try:
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
def insert_historical_data(candlestick_data):
    """
    Fonction pour insérer des données historiques de cryptomonnaies dans la base de données.

    Arguments :
    - `candlestick_data` (list of tuples) : Liste des tuples contenant les données de chandeliers (OHLC).

    Description :
    - Insère les données historiques de cryptomonnaies (chandeliers) dans la base de données via une requête SQL.

    Retour :
    - Aucun.
    """
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

# Fonction pour stocker les données en temps réel de Binance
def insert_stream_crypto_data(stream_data):
    """
    Fonction pour insérer des données de flux en temps réel (streaming) de Binance dans la base de données.

    Arguments :
    - `stream_data` (dict) : Dictionnaire contenant les données de flux en temps réel (OHLC, volume, nombre de transactions, etc.).

    Description :
    - Insère ou met à jour les données en temps réel dans la table `stream_crypto_data`.

    Retour :
    - Aucun.
    """
    try:
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

        __execute_query_to_one_value(query, values)

    except Exception as e:
        print(f"Error inserting stream crypto data: {e}")
        raise
