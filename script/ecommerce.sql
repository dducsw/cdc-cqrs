--/home/ldduc/Downloads/ecommerce.sql
-- PostgreSQL database dump

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

SET default_tablespace = '';
SET default_with_oids = false;


DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    address VARCHAR(100) NOT NULL,
    phone VARCHAR(10) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(50) DEFAULT 'pending'
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price NUMERIC(10,2) NOT NULL,
    stock_quantity INT NOT NULL
);

CREATE TABLE order_details (
    order_detail_id SERIAL PRIMARY KEY,
    order_id SMALLINT NOT NULL,
    product_id SMALLINT NOT NULL,
    unit_price REAL NOT NULL,
    quantity SMALLINT NOT NULL,
    discount REAL NOT NULL
);

CREATE TABLE test (
    number SMALLINT
);

INSERT INTO orders (customer_name, address, phone, order_date, status) VALUES
('Nguyen Van A', '123 Le Loi, Hanoi', '0912345678', '2025-08-01 09:00:00', 'pending'),
('Tran Thi B', '45 Nguyen Hue, HCMC', '0987654321', '2025-08-02 10:30:00', 'shipped'),
('Le Van C', '78 Bach Dang, Danang', '0901234567', '2025-08-03 14:15:00', 'delivered'),
('Pham Thi D', '89 Tran Hung Dao, Can Tho', '0934567890', '2025-08-04 11:00:00', 'pending'),
('Hoang Van E', '56 Vo Thi Sau, Hue', '0923456789', '2025-08-05 08:45:00', 'cancelled');

INSERT INTO products (name, description, price, stock_quantity) VALUES
('Laptop ASUS X515', 'Laptop 15.6 inch, i5, 8GB RAM, 512GB SSD', 15000000.00, 25),
('iPhone 14 Pro', 'Apple smartphone 128GB, Purple', 28000000.00, 10),
('Samsung Galaxy A54', 'Mid-range smartphone 256GB', 11000000.00, 30),
('Bàn phím cơ Logitech', 'Bàn phím cơ không dây, RGB', 2500000.00, 50),
('Tai nghe Sony WH-1000XM4', 'Tai nghe chống ồn cao cấp', 7000000.00, 20);

INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES
(1, 1, 15000000, 1, 0.00),
(1, 4, 2500000, 2, 0.10),
(2, 2, 28000000, 1, 0.00),
(3, 3, 11000000, 1, 0.05),
(3, 5, 7000000, 1, 0.00),
(4, 4, 2500000, 1, 0.15),
(5, 1, 15000000, 1, 0.20);


ALTER TABLE order_details
ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE;


ALTER TABLE order_details
ADD CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES products(product_id);


ALTER ROLE debezium WITH REPLICATION;

GRANT CONNECT ON DATABASE ecommerce TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT CREATE ON DATABASE ecommerce TO debezium;
GRANT TRIGGER ON ALL TABLES IN SCHEMA public TO debezium;

CREATE PUBLICATION ecommerce_pub
    FOR TABLE orders, order_details, products, test;
