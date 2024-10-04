-- Initialisation de la base de données pour le projet CryptoBoat
-- ----------------------------------
-- Cette section initialise la base de données du projet CryptoBoat.

-- Création de la base de données (si nécessaire)
CREATE DATABASE cryptoboat_db;

-- Connexion à la base de données cryptoboat_db
\c cryptoboat_db;

-- Création des tables
-- ----------------------------------
-- Cette section crée les différentes tables nécessaires au projet.

-- Création de la table users
CREATE TABLE IF NOT EXISTS users (
    id_users SERIAL PRIMARY KEY,                              -- Identifiant unique pour chaque utilisateur
    username VARCHAR(50) NOT NULL,                            -- Nom d'utilisateur (obligatoire)
    email VARCHAR(100) NOT NULL,                              -- Adresse email de l'utilisateur (obligatoire)
    password_hash VARCHAR(255) NOT NULL,                      -- Hash du mot de passe (obligatoire)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,           -- Date de création de l'utilisateur
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP            -- Date de la dernière mise à jour
);

-- Ajout de l'utilisateur admin par défaut
-- Cet utilisateur admin par défaut est créé si aucun utilisateur n'existe.
INSERT INTO users (id_users, username, email, password_hash, created_at, updated_at)
VALUES (1, 'admin', 'opa2024dst@gmail.com', crypt('adminopa2024', gen_salt('bf')), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id_users) DO NOTHING;

-- Création de la table crypto_characteristics
CREATE TABLE IF NOT EXISTS crypto_characteristics (
    id_crypto_characteristics SERIAL PRIMARY KEY,             -- Identifiant unique pour chaque cryptomonnaie
    name VARCHAR(50) NOT NULL,                                -- Nom de la cryptomonnaie (ex : Bitcoin)
    symbol VARCHAR(16) NOT NULL,                              -- Symbole de la cryptomonnaie (ex : BTC)
    market_cap BIGINT,                                        -- Capitalisation boursière
    circulating_supply FLOAT,                                 -- Offre circulante (quantité de cryptomonnaie en circulation)
    max_supply FLOAT,                                         -- Offre maximale (quantité maximale qui sera produite)
    CONSTRAINT unique_symbol UNIQUE (symbol)                  -- Contrainte d'unicité sur le symbole pour éviter les doublons
);

-- Création de la table intervals
CREATE TABLE IF NOT EXISTS intervals (
    id_interval SERIAL PRIMARY KEY,                           -- Identifiant unique pour chaque intervalle de temps
    intervals VARCHAR(4) NOT NULL                             -- Valeur de l'intervalle (ex : '15m', '1d')
);

-- Création de la table historical_crypto_data
CREATE TABLE IF NOT EXISTS historical_crypto_data (
    id_historical_crypto_data SERIAL PRIMARY KEY,             -- Identifiant unique pour chaque entrée historique
    id_crypto_characteristics INTEGER NOT NULL,               -- Référence à l'identifiant de la cryptomonnaie
    id_interval INTEGER NOT NULL,                             -- Référence à l'intervalle de temps
    open_time BIGINT NOT NULL,                                -- Timestamp d'ouverture de la bougie
    open_price FLOAT NOT NULL,                                -- Prix d'ouverture
    high_price FLOAT NOT NULL,                                -- Prix le plus haut
    low_price FLOAT NOT NULL,                                 -- Prix le plus bas
    close_price FLOAT NOT NULL,                               -- Prix de clôture
    volume FLOAT NOT NULL,                                    -- Volume échangé pendant cet intervalle
    close_time BIGINT NOT NULL,                               -- Timestamp de clôture de la bougie
    quote_asset_volume FLOAT NOT NULL,                        -- Volume de l'actif de cotation échangé
    number_of_trades FLOAT NOT NULL,                          -- Nombre de transactions
    taker_buy_base_asset_volume FLOAT NOT NULL,               -- Volume d'achat des takers en actif de base
    taker_buy_quote_asset_volume FLOAT NOT NULL,              -- Volume d'achat des takers en actif de cotation
    FOREIGN KEY (id_crypto_characteristics) REFERENCES crypto_characteristics (id_crypto_characteristics),  -- Référence à la table crypto_characteristics
    FOREIGN KEY (id_interval) REFERENCES intervals (id_interval), -- Référence à la table intervals
    CONSTRAINT unique_historical_crypto_data UNIQUE (id_crypto_characteristics, id_interval, open_time) -- Contrainte d'unicité sur l'ID crypto, intervalle et open_time
);

-- Création de la table stream_crypto_data
CREATE TABLE IF NOT EXISTS stream_crypto_data (
    id_stream_crypto_data SERIAL PRIMARY KEY,                 -- Identifiant unique pour chaque entrée de flux
    id_crypto_characteristics INTEGER NOT NULL,               -- Référence à l'identifiant de la cryptomonnaie
    event_time BIGINT NOT NULL,                               -- Timestamp de l'événement
    first_trade_id BIGINT NOT NULL,                           -- Identifiant du premier trade dans la bougie
    last_trade_id BIGINT NOT NULL,                            -- Identifiant du dernier trade dans la bougie
    open_time BIGINT NOT NULL,                                -- Timestamp d'ouverture de la bougie
    open_price FLOAT NOT NULL,                                -- Prix d'ouverture
    high_price FLOAT NOT NULL,                                -- Prix le plus haut
    low_price FLOAT NOT NULL,                                 -- Prix le plus bas
    close_price FLOAT NOT NULL,                               -- Prix de clôture
    close_time BIGINT NOT NULL,                               -- Timestamp de clôture de la bougie
    base_asset_volume FLOAT NOT NULL,                         -- Volume de l'actif de base échangé
    number_of_trades FLOAT NOT NULL,                          -- Nombre de transactions
    is_this_kline_closed BOOLEAN NOT NULL,                    -- Indique si la bougie est fermée
    quote_asset_volume FLOAT NOT NULL,                        -- Volume de l'actif de cotation échangé
    taker_buy_base_asset_volume FLOAT NOT NULL,               -- Volume d'achat des takers en actif de base
    taker_buy_quote_asset_volume FLOAT NOT NULL,              -- Volume d'achat des takers en actif de cotation
    FOREIGN KEY (id_crypto_characteristics) REFERENCES crypto_characteristics (id_crypto_characteristics), -- Référence à la table crypto_characteristics
    CONSTRAINT unique_stream_crypto_data UNIQUE (id_crypto_characteristics)  -- Contrainte d'unicité sur l'ID crypto pour éviter les doublons
);

-- Insertion des valeurs pour les intervalles
-- Cette section insère des valeurs prédéfinies pour les intervalles de temps dans la table intervals.
INSERT INTO intervals (intervals)
VALUES
    ('15m'),   -- Intervalle de 15 minutes (scalping)
    ('1h'),    -- Intervalle d'une heure
    ('4h'),    -- Intervalle de 4 heures
    ('1d'),    -- Intervalle d'une journée
    ('1w'),    -- Intervalle d'une semaine
    ('1M');    -- Intervalle d'un mois
