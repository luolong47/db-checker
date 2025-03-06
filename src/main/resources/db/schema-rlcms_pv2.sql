-- rlcms_pv2数据库表结构
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
  id INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  phone VARCHAR(20),
  email VARCHAR(100) NOT NULL,
  address VARCHAR(200),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 订阅表结构
DROP TABLE IF EXISTS subscriptions;

CREATE TABLE subscriptions (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  plan_name VARCHAR(50) NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE,
  status VARCHAR(20) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TEST4表结构
DROP TABLE IF EXISTS TEST4;

CREATE TABLE TEST4 (
  id INT AUTO_INCREMENT PRIMARY KEY,
  value1 VARCHAR(100),
  value2 DECIMAL(15,2),
  value3 TIMESTAMP,
  value4 BOOLEAN,
  value5 VARCHAR(200),
  value6 INT,
  value7 DECIMAL(10,4),
  value8 VARCHAR(50),
  value9 DATE,
  value10 TEXT
); 