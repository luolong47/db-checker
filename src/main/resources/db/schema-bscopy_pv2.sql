-- bscopy_pv2数据库表结构
DROP TABLE IF EXISTS inventory;

CREATE TABLE inventory (
  id INT AUTO_INCREMENT PRIMARY KEY,
  sku VARCHAR(50) NOT NULL,
  product_name VARCHAR(100) NOT NULL,
  category VARCHAR(50),
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2),
  supplier_id INT,
  warehouse_id INT,
  reorder_level INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP TABLE IF EXISTS products;

CREATE TABLE products
(
    id         INT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(100)   NOT NULL,
    price      DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- 仓库表结构
DROP TABLE IF EXISTS warehouses;

CREATE TABLE warehouses (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  location VARCHAR(200),
  capacity INT,
  manager_id INT,
  contact_phone VARCHAR(20),
  active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TEST7表结构
DROP TABLE IF EXISTS TEST7;

CREATE TABLE TEST7 (
  id INT AUTO_INCREMENT PRIMARY KEY,
  transaction_id VARCHAR(50),
  transaction_date TIMESTAMP,
  customer_id INT,
  amount DECIMAL(15,2),
  payment_method VARCHAR(20),
  status VARCHAR(20),
  notes TEXT,
  processed_by INT,
  reference_no VARCHAR(50),
  batch_id VARCHAR(30)
); 