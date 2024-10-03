-- Initialisation de la base de données pour le projet CryptoBoat
-- ----------------------------------

-- Création de la base de données (si nécessaire)
CREATE DATABASE cryptoboat_db;

-- Connexion à la base de données cryptoboat_db
\c cryptoboat_db;

-- Création des tables
-- ----------------------------------

-- Création de la table users
CREATE TABLE IF NOT EXISTS users (
    id_users SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_users UNIQUE (id_users, username)
);

-- Création de la table crypto_characteristics
CREATE TABLE IF NOT EXISTS crypto_characteristics (
    id_crypto_characteristics SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    symbol VARCHAR(16) NOT NULL,
    market_cap BIGINT,
    circulating_supply FLOAT,
    max_supply FLOAT,
    CONSTRAINT unique_symbol UNIQUE (symbol)
);

-- Création de la table intervals
CREATE TABLE IF NOT EXISTS intervals (
    id_interval SERIAL PRIMARY KEY,
    intervals VARCHAR(4) NOT NULL,
    CONSTRAINT unique_intervals UNIQUE (id_interval, intervals)
);

-- Création de la table historical_crypto_data
CREATE TABLE IF NOT EXISTS historical_crypto_data (
    id_historical_crypto_data SERIAL PRIMARY KEY,
    id_crypto_characteristics INTEGER NOT NULL,
    id_interval INTEGER NOT NULL,
    open_time BIGINT NOT NULL,
    open_price FLOAT NOT NULL,
    high_price FLOAT NOT NULL,
    low_price FLOAT NOT NULL,
    close_price FLOAT NOT NULL,
    volume FLOAT NOT NULL,
    close_time BIGINT NOT NULL,
    quote_asset_volume FLOAT NOT NULL,
    number_of_trades FLOAT NOT NULL,
    taker_buy_base_asset_volume FLOAT NOT NULL,
    taker_buy_quote_asset_volume FLOAT NOT NULL,
    FOREIGN KEY (id_crypto_characteristics) REFERENCES crypto_characteristics (id_crypto_characteristics),
    FOREIGN KEY (id_interval) REFERENCES intervals (id_interval),
    CONSTRAINT unique_historical_crypto_data UNIQUE (id_crypto_characteristics, id_interval, open_time)
);

-- Création de la table stream_crypto_data
CREATE TABLE IF NOT EXISTS stream_crypto_data (
    id_stream_crypto_data SERIAL PRIMARY KEY,
    id_crypto_characteristics INTEGER NOT NULL,
    event_time BIGINT NOT NULL,
    first_trade_id BIGINT NOT NULL,
    last_trade_id BIGINT NOT NULL,
    open_time BIGINT NOT NULL,
    open_price FLOAT NOT NULL,
    high_price FLOAT NOT NULL,
    low_price FLOAT NOT NULL,
    close_price FLOAT NOT NULL,
    close_time BIGINT NOT NULL,
    base_asset_volume FLOAT NOT NULL,
    number_of_trades FLOAT NOT NULL,
    is_this_kline_closed BOOLEAN NOT NULL,
    quote_asset_volume FLOAT NOT NULL,
    taker_buy_base_asset_volume FLOAT NOT NULL,
    taker_buy_quote_asset_volume FLOAT NOT NULL,
    FOREIGN KEY (id_crypto_characteristics) REFERENCES crypto_characteristics (id_crypto_characteristics),
    CONSTRAINT unique_stream_crypto_data UNIQUE (id_crypto_characteristics)
);


-- ALTER TABLE users ADD CONSTRAINT unique_users UNIQUE (id_users, username);
-- ALTER TABLE intervals ADD CONSTRAINT unique_intervals UNIQUE (id_interval, intervals);

-- Insertion des valeurs pour les intervalles
INSERT INTO intervals (id_interval, intervals)
VALUES
    (1, '15m'),   -- scalping
    (2, '1h'),    -- horaire
    (3, '4h'),    -- horaire
    (4, '1d'),    -- journalier
    (5, '1w'),    -- hebdomadaire
    (6, '1M')    -- mensuel
ON CONFLICT (id_interval, intervals) DO NOTHING;

INSERT INTO users (id_users, username, email, password_hash)
VALUES
    (1, 'admin', 'admin@localhost.local', '8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918')
ON CONFLICT (id_users, username) DO NOTHING;
