CREATE TABLE customer_pet
(
    id       INTEGER PRIMARY KEY,
    type     VARCHAR NOT NULL,
    name     VARCHAR NOT NULL,
    breed    VARCHAR NOT NULL,
    category VARCHAR NOT NULL
);
CREATE TABLE customer
(
    id              INTEGER PRIMARY KEY,
    first_name      VARCHAR NOT NULL,
    last_name       VARCHAR NOT NULL,
    age             INTEGER NOT NULL,
    email           VARCHAR NOT NULL,
    country         VARCHAR NOT NULL,
    postal_code     VARCHAR,
    customer_pet_id INTEGER REFERENCES customer_pet (id)
);
CREATE TABLE seller
(
    id          INTEGER PRIMARY KEY,
    first_name  VARCHAR NOT NULL,
    last_name   VARCHAR NOT NULL,
    email       VARCHAR NOT NULL,
    country     VARCHAR NOT NULL,
    postal_code VARCHAR
);
CREATE TABLE store
(
    id       INTEGER PRIMARY KEY,
    name     VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    city     VARCHAR NOT NULL,
    state    VARCHAR,
    country  VARCHAR NOT NULL,
    phone    VARCHAR NOT NULL,
    email    VARCHAR NOT NULL
);
CREATE TABLE supplier
(
    id      INTEGER PRIMARY KEY,
    name    VARCHAR NOT NULL,
    contact VARCHAR NOT NULL,
    email   VARCHAR NOT NULL,
    phone   VARCHAR NOT NULL,
    address VARCHAR NOT NULL,
    city    VARCHAR NOT NULL,
    country VARCHAR NOT NULL
);

CREATE TABLE product
(
    id                 INTEGER PRIMARY KEY,
    name               VARCHAR        NOT NULL,
    category           VARCHAR        NOT NULL,
    price              NUMERIC(10, 2) NOT NULL,
    available_quantity INTEGER        NOT NULL,
    weight             NUMERIC(10, 1) NOT NULL,
    color              VARCHAR        NOT NULL,
    size               VARCHAR        NOT NULL,
    brand              VARCHAR        NOT NULL,
    material           VARCHAR        NOT NULL,
    description        VARCHAR        NOT NULL,
    rating             NUMERIC(2, 1)  NOT NULL,
    reviews            INTEGER        NOT NULL,
    release_date       DATE           NOT NULL,
    expiry_date        DATE           NOT NULL,
    supplier_id        INTEGER        NOT NULL REFERENCES supplier (id)
);

CREATE TABLE sales_fact
(
    id            INTEGER PRIMARY KEY,
    customer_id   INTEGER        NOT NULL REFERENCES customer (id),
    seller_id     INTEGER        NOT NULL REFERENCES seller (id),
    store_id      INTEGER        NOT NULL REFERENCES store (id),
    product_id    INTEGER        NOT NULL REFERENCES product (id),
    date          DATE           NOT NULL,
    sale_quantity INTEGER        NOT NULL,
    total_price   NUMERIC(12, 2) NOT NULL
);