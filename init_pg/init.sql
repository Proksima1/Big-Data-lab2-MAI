\set ON_ERROR_STOP on

DROP TABLE IF EXISTS input_csv;

CREATE TABLE input_csv
(
    id                   TEXT,
    customer_first_name  TEXT,
    customer_last_name   TEXT,
    customer_age         TEXT,
    customer_email       TEXT,
    customer_country     TEXT,
    customer_postal_code TEXT,
    customer_pet_type    TEXT,
    customer_pet_name    TEXT,
    customer_pet_breed   TEXT,
    seller_first_name    TEXT,
    seller_last_name     TEXT,
    seller_email         TEXT,
    seller_country       TEXT,
    seller_postal_code   TEXT,
    product_name         TEXT,
    product_category     TEXT,
    product_price        TEXT,
    product_quantity     TEXT,
    sale_date            TEXT,
    sale_customer_id     TEXT,
    sale_seller_id       TEXT,
    sale_product_id      TEXT,
    sale_quantity        TEXT,
    sale_total_price     TEXT,
    store_name           TEXT,
    store_location       TEXT,
    store_city           TEXT,
    store_state          TEXT,
    store_country        TEXT,
    store_phone          TEXT,
    store_email          TEXT,
    pet_category         TEXT,
    product_weight       TEXT,
    product_color        TEXT,
    product_size         TEXT,
    product_brand        TEXT,
    product_material     TEXT,
    product_description  TEXT,
    product_rating       TEXT,
    product_reviews      TEXT,
    product_release_date TEXT,
    product_expiry_date  TEXT,
    supplier_name        TEXT,
    supplier_contact     TEXT,
    supplier_email       TEXT,
    supplier_phone       TEXT,
    supplier_address     TEXT,
    supplier_city        TEXT,
    supplier_country     TEXT
);

-- Optional: clear table explicitly if you later change DROP TABLE to CREATE IF NOT EXISTS.
-- TRUNCATE TABLE stage_sales;

\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_01.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_02.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_03.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_04.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_05.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_06.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_07.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_08.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_09.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');
\copy input_csv FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA_10.csv' WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

-- Quick load check
SELECT COUNT(*) AS loaded_rows FROM input_csv;