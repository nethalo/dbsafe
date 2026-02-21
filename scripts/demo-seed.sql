-- =============================================================================
-- dbsafe demo environment seed script
--
-- Creates an e-commerce database with realistic data volumes:
--   customers    10,000 rows  (~3 MB)   — FK target for orders
--   products      1,000 rows  (~1 MB)   — FK target for order_items
--   orders      ~2.56M rows  (~1.3 GB)  — star table; triggers DANGEROUS DDL risk
--   order_items  ~500K rows  (~50 MB)   — 2 FK constraints; rich FK display
--   audit_log    ~250K rows  (~50 MB)   — DML demo; triggers CHUNKED recommendation
--
-- Run time: ~3-5 minutes
--
-- Doubling strategy: a stored procedure commits every 100K rows so that each
-- transaction only needs ~60 MB of redo log — safe on any redo log capacity.
-- =============================================================================

SET SESSION foreign_key_checks = 0;

-- ---------------------------------------------------------------------------
-- Database + user setup
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS demo CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci;
USE demo;

CREATE USER IF NOT EXISTS 'dbsafe'@'%' IDENTIFIED BY 'dbsafe_demo';
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'dbsafe'@'%';
GRANT ALL PRIVILEGES ON demo.* TO 'dbsafe'@'%';
FLUSH PRIVILEGES;

-- ---------------------------------------------------------------------------
-- Helper table: cross-joins generate all other data
-- ---------------------------------------------------------------------------
CREATE TABLE digits (n TINYINT UNSIGNED PRIMARY KEY);
INSERT INTO digits VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

-- =============================================================================
-- TABLE: customers (10,000 rows)
-- =============================================================================
CREATE TABLE customers (
  id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
  email       VARCHAR(200) NOT NULL,
  first_name  VARCHAR(50)  NOT NULL,
  last_name   VARCHAR(50)  NOT NULL,
  phone       VARCHAR(20),
  address     VARCHAR(200),
  city        VARCHAR(100),
  state       CHAR(2),
  zip         VARCHAR(10),
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_email       (email),
  KEY        idx_created    (created_at),
  KEY        idx_city_state (city, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

INSERT INTO customers
  (email, first_name, last_name, phone, address, city, state, zip, created_at)
SELECT
  CONCAT('user', d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, '@example.com'),
  ELT(1 + MOD(d1.n * 37 + d2.n * 13, 10),
      'Alice','Bob','Charlie','Diana','Eve','Frank','Grace','Henry','Ivy','Jack'),
  ELT(1 + MOD(d3.n * 41 + d4.n * 17, 10),
      'Smith','Jones','Brown','Davis','Wilson','Moore','Taylor','Anderson','Thomas','Jackson'),
  CONCAT('+1-555-', LPAD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 4, '0')),
  CONCAT(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n + 1, ' ',
         ELT(1 + MOD(d1.n * 7 + d3.n, 5), 'Main', 'Oak', 'Maple', 'Pine', 'Cedar'), ' ',
         ELT(1 + MOD(d2.n * 7 + d4.n, 4), 'St', 'Ave', 'Blvd', 'Dr')),
  ELT(1 + MOD(d1.n * 31 + d2.n * 19 + d3.n * 7 + d4.n, 8),
      'Springfield','Shelbyville','Ogdenville','North Haverbrook',
      'Capital City','Brockway','Waverly Hills','Cypress Creek'),
  ELT(1 + MOD(d1.n * 11 + d4.n * 23, 10),
      'IL','CA','TX','NY','FL','WA','OH','GA','NC','PA'),
  LPAD(60000 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 40000), 5, '0'),
  DATE_SUB(NOW(), INTERVAL MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 1095) DAY)
FROM digits d1, digits d2, digits d3, digits d4;

-- =============================================================================
-- TABLE: products (1,000 rows)
-- =============================================================================
CREATE TABLE products (
  id             INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  sku            VARCHAR(50)   NOT NULL,
  name           VARCHAR(200)  NOT NULL,
  description    TEXT,
  price          DECIMAL(10,2) NOT NULL,
  cost           DECIMAL(10,2),
  stock_quantity INT           NOT NULL DEFAULT 0,
  category       VARCHAR(100),
  brand          VARCHAR(100),
  weight_kg      DECIMAL(6,3),
  created_at     DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uq_sku       (sku),
  KEY        idx_category (category),
  KEY        idx_brand    (brand),
  KEY        idx_price    (price)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

INSERT INTO products
  (sku, name, description, price, cost, stock_quantity, category, brand, weight_kg)
SELECT
  CONCAT('SKU-', LPAD(d1.n * 100 + d2.n * 10 + d3.n + 1, 4, '0')),
  CONCAT(
    ELT(1 + MOD(d1.n * 37 + d2.n, 5), 'Wireless ', 'Premium ', 'Smart ', 'Classic ', 'Ultra '),
    ELT(1 + MOD(d1.n * 13 + d2.n * 7 + d3.n, 8),
        'Headphones','Keyboard','Mouse','Monitor','Laptop','Tablet','Webcam','Speaker')
  ),
  CONCAT('High-quality ',
    ELT(1 + MOD(d1.n * 13 + d2.n * 7 + d3.n, 8),
        'headphones','keyboard','mouse','monitor','laptop','tablet','webcam','speaker'),
    ' suitable for everyday professional use.'),
  ROUND(9.99 + MOD(d1.n * 100 + d2.n * 10 + d3.n, 200) * 4.99, 2),
  ROUND(5.00 + MOD(d1.n * 100 + d2.n * 10 + d3.n, 200) * 2.50, 2),
  MOD(d1.n * 100 + d2.n * 10 + d3.n, 500) + 10,
  ELT(1 + MOD(d1.n * 100 + d2.n * 10 + d3.n, 5),
      'Electronics','Computers','Peripherals','Mobile','Audio'),
  ELT(1 + MOD(d1.n * 37 + d3.n * 17, 8),
      'TechPro','LogiMax','SoundWave','ViewClear','CoreCompute','MobileEdge','PixelPerfect','AudioElite'),
  ROUND(0.1 + MOD(d1.n * 100 + d2.n * 10 + d3.n, 50) * 0.05, 3)
FROM digits d1, digits d2, digits d3;

-- =============================================================================
-- TABLE: orders (~2.56M rows, ~1.3 GB)
--
-- Uses utf8mb3 charset deliberately so that:
--   ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4
-- requires COPY algorithm on a >1 GB table → DANGEROUS + gh-ost/pt-osc output.
--
-- Build strategy:
--   1. Seed 10,000 rows via 4-way cross-join on digits
--   2. Double 8 times using a stored procedure that commits every 100K rows
--      (keeps each transaction small → no redo log overflow)
--   3. Fix order_number uniqueness in batches, then add the UNIQUE index
-- =============================================================================
CREATE TABLE orders (
  id               INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  order_number     VARCHAR(20)   NOT NULL,
  customer_id      INT UNSIGNED  NOT NULL,
  status           VARCHAR(20)   NOT NULL DEFAULT 'pending',
  total_amount     DECIMAL(12,2) NOT NULL,
  subtotal         DECIMAL(12,2) NOT NULL,
  tax_amount       DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  shipping_cost    DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  discount_amount  DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  payment_method   VARCHAR(50)   NOT NULL,
  payment_status   VARCHAR(20)   NOT NULL DEFAULT 'pending',
  shipping_name    VARCHAR(150)  NOT NULL,
  shipping_address VARCHAR(300)  NOT NULL,
  billing_address  VARCHAR(300)  NOT NULL,
  tracking_number  VARCHAR(100),
  ip_address       VARCHAR(45),
  user_agent       VARCHAR(250),
  created_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  shipped_at       DATETIME,
  delivered_at     DATETIME,
  PRIMARY KEY (id),
  -- UNIQUE KEY added after data load (see ALTER TABLE below)
  KEY idx_order_number  (order_number),
  KEY idx_customer_id   (customer_id),
  KEY idx_status        (status),
  KEY idx_created_at    (created_at),
  KEY idx_payment_method(payment_method),
  KEY idx_status_created(status, created_at),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- ---------------------------------------------------------------------------
-- Seed: 10,000 orders via 4-way cross-join
-- ---------------------------------------------------------------------------
INSERT INTO orders
  (order_number, customer_id, status, total_amount, subtotal, tax_amount,
   shipping_cost, discount_amount, payment_method, payment_status,
   shipping_name, shipping_address, billing_address,
   tracking_number, ip_address, user_agent, created_at, updated_at, shipped_at)
SELECT
  CONCAT('INIT-', LPAD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 8, '0')),
  1 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 10000),
  ELT(1 + MOD(d1.n * 37 + d2.n * 17 + d3.n * 7 + d4.n, 5),
      'pending','processing','shipped','delivered','cancelled'),
  ROUND(19.99 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 500) * 2.99, 2),
  ROUND(17.99 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 500) * 2.79, 2),
  ROUND(1.44  + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 500) * 0.224, 2),
  CASE MOD(d1.n * 7 + d4.n * 3, 3) WHEN 0 THEN 0.00 WHEN 1 THEN 5.99 ELSE 12.99 END,
  0.00,
  ELT(1 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 4),
      'credit_card','debit_card','paypal','bank_transfer'),
  ELT(1 + MOD(d1.n * 100 + d2.n * 10 + d3.n, 3), 'paid','pending','failed'),
  CONCAT(
    ELT(1 + MOD(d1.n * 37 + d2.n * 13, 10),
        'Alice','Bob','Charlie','Diana','Eve','Frank','Grace','Henry','Ivy','Jack'),
    ' ',
    ELT(1 + MOD(d3.n * 41 + d4.n * 17, 10),
        'Smith','Jones','Brown','Davis','Wilson','Moore','Taylor','Anderson','Thomas','Jackson')
  ),
  CONCAT(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n + 1, ' ',
    ELT(1 + MOD(d1.n * 31 + d2.n * 19 + d3.n * 7, 5), 'Main', 'Oak', 'Maple', 'Pine', 'Cedar'),
    ' St, ',
    ELT(1 + MOD(d1.n * 29 + d2.n * 11 + d4.n, 6),
        'Springfield','Shelbyville','Capital City','Ogdenville','Brockway','North Haverbrook'),
    ', IL ', LPAD(60000 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 9999), 5, '0')),
  CONCAT(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n + 1, ' ',
    ELT(1 + MOD(d2.n * 23 + d3.n * 13 + d4.n, 5), 'Elm', 'Birch', 'Walnut', 'Ash', 'Willow'),
    ' Ave, ',
    ELT(1 + MOD(d2.n * 29 + d3.n * 11 + d4.n * 23, 6),
        'Springfield','Shelbyville','Capital City','Ogdenville','Brockway','North Haverbrook'),
    ', IL ', LPAD(61000 + MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 9999), 5, '0')),
  CASE WHEN MOD(d1.n * 10 + d4.n, 3) = 0
       THEN CONCAT('1Z', LPAD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 16, '0'))
       ELSE NULL END,
  CONCAT(MOD(d1.n * 50 + 10, 223), '.', MOD(d2.n * 30 + d3.n * 15 + 168, 254), '.',
         d3.n * 10 + d4.n, '.', d1.n * d4.n + 1),
  CONCAT('Mozilla/5.0 (',
    ELT(1 + MOD(d1.n * 3 + d2.n, 3),
        'Windows NT 10.0; Win64; x64',
        'Macintosh; Intel Mac OS X 10_15_7',
        'X11; Linux x86_64'),
    ') AppleWebKit/537.36 (KHTML, like Gecko) Chrome/', 90 + d3.n, '.0 Safari/537.36'),
  DATE_SUB(NOW(), INTERVAL MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 730) DAY),
  DATE_ADD(
    DATE_SUB(NOW(), INTERVAL MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 730) DAY),
    INTERVAL 1 DAY),
  CASE WHEN MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 5) IN (2, 3)
       THEN DATE_ADD(
              DATE_SUB(NOW(), INTERVAL MOD(d1.n * 1000 + d2.n * 100 + d3.n * 10 + d4.n, 720) DAY),
              INTERVAL 3 DAY)
       ELSE NULL END
FROM digits d1, digits d2, digits d3, digits d4;

-- =============================================================================
-- Stored procedures for safe batched doubling
--
-- Each batch is 100K rows → ~60 MB redo log per COMMIT.
-- The WHERE id BETWEEN clause uses v_max_id captured at procedure START,
-- so newly-inserted rows (id > v_max_id) are never re-read as source rows.
-- =============================================================================
DELIMITER $$

CREATE PROCEDURE demo_double_orders()
BEGIN
  DECLARE v_max_id BIGINT UNSIGNED DEFAULT 0;
  DECLARE v_start  BIGINT UNSIGNED DEFAULT 1;
  DECLARE v_end    BIGINT UNSIGNED;
  DECLARE v_step   BIGINT UNSIGNED DEFAULT 100000;

  SELECT MAX(id) INTO v_max_id FROM orders;

  WHILE v_start <= v_max_id DO
    SET v_end = LEAST(v_start + v_step - 1, v_max_id);

    INSERT INTO orders
      (order_number, customer_id, status, total_amount, subtotal, tax_amount,
       shipping_cost, discount_amount, payment_method, payment_status,
       shipping_name, shipping_address, billing_address,
       tracking_number, ip_address, user_agent, created_at, updated_at, shipped_at)
    SELECT
      order_number, customer_id, status, total_amount, subtotal, tax_amount,
      shipping_cost, discount_amount, payment_method, payment_status,
      shipping_name, shipping_address, billing_address,
      tracking_number, ip_address, user_agent, created_at, updated_at, shipped_at
    FROM orders
    WHERE id BETWEEN v_start AND v_end;

    COMMIT;
    SET v_start = v_start + v_step;
  END WHILE;
END$$

CREATE PROCEDURE demo_fix_order_numbers()
BEGIN
  DECLARE v_max_id BIGINT UNSIGNED DEFAULT 0;
  DECLARE v_start  BIGINT UNSIGNED DEFAULT 1;
  DECLARE v_step   BIGINT UNSIGNED DEFAULT 100000;

  SELECT MAX(id) INTO v_max_id FROM orders;

  WHILE v_start <= v_max_id DO
    UPDATE orders
    SET    order_number = CONCAT('ORD-', LPAD(id, 8, '0'))
    WHERE  id BETWEEN v_start AND LEAST(v_start + v_step - 1, v_max_id);

    COMMIT;
    SET v_start = v_start + v_step;
  END WHILE;
END$$

DELIMITER ;

-- ---------------------------------------------------------------------------
-- Double 8 times: 10K → 20K → 40K → 80K → 160K → 320K → 640K → 1.28M → 2.56M
-- ---------------------------------------------------------------------------
CALL demo_double_orders();
CALL demo_double_orders();
CALL demo_double_orders();
CALL demo_double_orders();
CALL demo_double_orders();
CALL demo_double_orders();
CALL demo_double_orders();
CALL demo_double_orders();

-- ---------------------------------------------------------------------------
-- Fix order_number to be unique based on auto_increment id
-- ---------------------------------------------------------------------------
CALL demo_fix_order_numbers();

DROP PROCEDURE demo_double_orders;
DROP PROCEDURE demo_fix_order_numbers;

-- ---------------------------------------------------------------------------
-- Promote idx_order_number to a UNIQUE constraint (all values now distinct).
-- InnoDB builds this index in one sorted pass — faster than per-insert checks.
-- ---------------------------------------------------------------------------
ALTER TABLE orders DROP INDEX idx_order_number;
ALTER TABLE orders ADD UNIQUE KEY uq_order_number (order_number);

-- =============================================================================
-- TABLE: order_items (~500K rows — 2 FK constraints for rich FK display demo)
-- =============================================================================
CREATE TABLE order_items (
  id               INT UNSIGNED  NOT NULL AUTO_INCREMENT,
  order_id         INT UNSIGNED  NOT NULL,
  product_id       INT UNSIGNED  NOT NULL,
  quantity         INT           NOT NULL DEFAULT 1,
  unit_price       DECIMAL(10,2) NOT NULL,
  line_total       DECIMAL(12,2) NOT NULL,
  discount_percent DECIMAL(5,2)  NOT NULL DEFAULT 0.00,
  created_at       DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_order_id  (order_id),
  KEY idx_product_id(product_id),
  CONSTRAINT fk_items_order   FOREIGN KEY (order_id)   REFERENCES orders   (id),
  CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products  (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- One item per order for the first 500K orders (range scan on PRIMARY KEY)
INSERT INTO order_items
  (order_id, product_id, quantity, unit_price, line_total, discount_percent)
SELECT
  id,
  1 + MOD(id - 1, 1000),
  1 + MOD(id, 4),
  ROUND(9.99 + MOD(id, 200) * 4.99, 2),
  ROUND((1 + MOD(id, 4)) * (9.99 + MOD(id, 200) * 4.99), 2),
  ROUND(MOD(id, 30) * 0.5, 2)
FROM orders
WHERE id <= 500000;

-- =============================================================================
-- TABLE: audit_log (~250K rows — for DML chunking demo)
--
-- Date range spans ~720 days so that:
--   DELETE FROM audit_log WHERE created_at < '2025-06-01'
-- affects ~150K+ rows → DANGEROUS CHUNKED recommendation
-- =============================================================================
CREATE TABLE audit_log (
  id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
  event_type  VARCHAR(50)  NOT NULL,
  table_name  VARCHAR(100) NOT NULL,
  record_id   INT UNSIGNED,
  user_id     INT UNSIGNED,
  old_values  JSON,
  new_values  JSON,
  ip_address  VARCHAR(45),
  created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_event_type(event_type),
  KEY idx_table_name(table_name),
  KEY idx_created_at(created_at),
  KEY idx_user_id   (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed 100K rows via 5-way cross-join (10^5 = 100,000 rows exactly)
INSERT INTO audit_log
  (event_type, table_name, record_id, user_id, old_values, new_values, ip_address, created_at)
SELECT
  ELT(1 + MOD(d1.n * 10000 + d2.n * 1000 + d3.n * 100 + d4.n * 10 + d5.n, 5),
      'INSERT','UPDATE','DELETE','SELECT','LOGIN'),
  ELT(1 + MOD(d1.n * 10000 + d2.n * 1000 + d3.n * 100 + d4.n * 10 + d5.n, 4),
      'orders','customers','products','order_items'),
  1 + MOD(d1.n * 10000 + d2.n * 1000 + d3.n * 100 + d4.n * 10 + d5.n, 500000),
  1 + MOD(d1.n * 10000 + d2.n * 1000 + d3.n * 100 + d4.n * 10 + d5.n, 1000),
  '{"id": 1, "status": "pending", "total_amount": "199.99"}',
  '{"id": 1, "status": "processing", "total_amount": "199.99"}',
  CONCAT('192.168.', MOD(d3.n * 10 + d4.n, 254), '.', MOD(d1.n * 10 + d2.n, 254)),
  DATE_SUB(NOW(), INTERVAL MOD(d1.n * 10000 + d2.n * 1000 + d3.n * 100 + d4.n * 10 + d5.n, 720) DAY)
FROM digits d1, digits d2, digits d3, digits d4, digits d5;

-- Double to 200K
INSERT INTO audit_log
  (event_type, table_name, record_id, user_id, old_values, new_values, ip_address, created_at)
SELECT event_type, table_name, record_id, user_id, old_values, new_values, ip_address, created_at
FROM audit_log;

-- Add 50K more → total 250K
INSERT INTO audit_log
  (event_type, table_name, record_id, user_id, old_values, new_values, ip_address, created_at)
SELECT event_type, table_name, record_id, user_id, old_values, new_values, ip_address, created_at
FROM audit_log
LIMIT 50000;

-- =============================================================================
-- Triggers on orders (created AFTER data load to avoid per-row overhead)
--
-- Two triggers so that DML on orders shows trigger fire warnings in dbsafe:
--   AFTER UPDATE  → records status/amount changes to audit_log
--   AFTER DELETE  → records deletions to audit_log
-- =============================================================================
DELIMITER $$

CREATE TRIGGER trg_orders_after_update
AFTER UPDATE ON orders
FOR EACH ROW
BEGIN
  INSERT INTO audit_log
    (event_type, table_name, record_id, old_values, new_values)
  VALUES
    ('UPDATE', 'orders', NEW.id,
     JSON_OBJECT('status', OLD.status, 'total_amount', OLD.total_amount),
     JSON_OBJECT('status', NEW.status, 'total_amount', NEW.total_amount));
END$$

CREATE TRIGGER trg_orders_after_delete
AFTER DELETE ON orders
FOR EACH ROW
BEGIN
  INSERT INTO audit_log
    (event_type, table_name, record_id, old_values, new_values)
  VALUES
    ('DELETE', 'orders', OLD.id,
     JSON_OBJECT('status', OLD.status, 'total_amount', OLD.total_amount),
     NULL);
END$$

DELIMITER ;

-- =============================================================================
-- Re-enable constraints and compute optimizer statistics
-- =============================================================================
SET SESSION foreign_key_checks = 1;

-- ANALYZE TABLE updates information_schema.TABLES statistics so dbsafe
-- reports accurate table sizes and row counts
ANALYZE TABLE customers;
ANALYZE TABLE products;
ANALYZE TABLE orders;
ANALYZE TABLE order_items;
ANALYZE TABLE audit_log;

DROP TABLE digits;
