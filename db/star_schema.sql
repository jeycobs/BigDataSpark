DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;

CREATE TABLE d_customer (
  customer_sk   SERIAL PRIMARY KEY,
  customer_id   BIGINT UNIQUE,
  first_name    TEXT NOT NULL,
  last_name     TEXT NOT NULL,
  age           INT,
  email         TEXT,
  country       TEXT,
  postal_code   TEXT,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE d_customer IS 'Справочник: покупатели';
COMMENT ON COLUMN d_customer.customer_id IS 'Идентификатор клиента из исходной системы';

CREATE TABLE d_seller (
  seller_sk     SERIAL PRIMARY KEY,
  seller_id     BIGINT UNIQUE,
  first_name    TEXT NOT NULL,
  last_name     TEXT NOT NULL,
  email         TEXT,
  country       TEXT,
  postal_code   TEXT,
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE d_seller IS 'Справочник: продавцы';

CREATE TABLE d_product (
  product_sk     SERIAL PRIMARY KEY,
  product_id     BIGINT UNIQUE,
  product_name   TEXT NOT NULL,
  category       TEXT,
  weight         NUMERIC,
  color          TEXT,
  size           TEXT,
  brand          TEXT,
  material       TEXT,
  description    TEXT,
  rating         NUMERIC,
  reviews        INT,
  date_added     DATE,
  expiration     DATE,
  price          NUMERIC,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE d_product IS 'Справочник: товары';

CREATE TABLE d_store (
  store_sk       SERIAL PRIMARY KEY,
  store_name     TEXT UNIQUE,
  location       TEXT,
  city           TEXT,
  region         TEXT,
  country        TEXT,
  phone          TEXT,
  email          TEXT,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE d_store IS 'Справочник: магазины';

CREATE TABLE d_supplier (
  supplier_sk    SERIAL PRIMARY KEY,
  supplier_name  TEXT UNIQUE,
  contact_name   TEXT,
  email          TEXT,
  phone          TEXT,
  address        TEXT,
  city           TEXT,
  country        TEXT,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE d_supplier IS 'Справочник: поставщики';

CREATE TABLE d_date (
  date_sk        SERIAL PRIMARY KEY,
  date_value     DATE UNIQUE NOT NULL,
  year           INT NOT NULL,
  quarter        INT NOT NULL,
  month          INT NOT NULL,
  month_name     TEXT NOT NULL,
  day            INT NOT NULL,
  weekday        INT NOT NULL,
  week_num       INT NOT NULL,
  weekend_flag   BOOLEAN NOT NULL,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
COMMENT ON TABLE d_date IS 'Справочник: календарь дат';

CREATE TABLE fact_sales (
  sale_sk           SERIAL PRIMARY KEY,
  date_sk           INT NOT NULL,
  customer_sk       INT NOT NULL,
  seller_sk         INT NOT NULL,
  product_sk        INT NOT NULL,
  store_sk          INT NOT NULL,
  supplier_sk       INT NOT NULL,
  quantity_sold     INT,
  total_amount      NUMERIC,
  unit_price        NUMERIC,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT fk_date      FOREIGN KEY (date_sk)     REFERENCES d_date(date_sk),
  CONSTRAINT fk_customer  FOREIGN KEY (customer_sk) REFERENCES d_customer(customer_sk),
  CONSTRAINT fk_seller    FOREIGN KEY (seller_sk)   REFERENCES d_seller(seller_sk),
  CONSTRAINT fk_product   FOREIGN KEY (product_sk)  REFERENCES d_product(product_sk),
  CONSTRAINT fk_store     FOREIGN KEY (store_sk)    REFERENCES d_store(store_sk),
  CONSTRAINT fk_supplier  FOREIGN KEY (supplier_sk) REFERENCES d_supplier(supplier_sk)
);
