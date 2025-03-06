-- rlcms_pv3数据库表结构
DROP TABLE IF EXISTS departments;

CREATE TABLE departments (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  location VARCHAR(100),
  budget DECIMAL(15,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 员工表结构
DROP TABLE IF EXISTS employees;

CREATE TABLE employees (
  id INT AUTO_INCREMENT PRIMARY KEY,
  department_id INT NOT NULL,
  first_name VARCHAR(50) NOT NULL,
  last_name VARCHAR(50) NOT NULL,
  position VARCHAR(100),
  salary DECIMAL(10,2),
  hire_date DATE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TEST5表结构
DROP TABLE IF EXISTS TEST5;

CREATE TABLE TEST5 (
  id INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(20),
  name VARCHAR(100),
  category VARCHAR(50),
  active BOOLEAN,
  price DECIMAL(10,2),
  discount DECIMAL(5,2),
  stock INT,
  min_stock INT,
  supplier_id INT,
  last_order_date DATE
); 