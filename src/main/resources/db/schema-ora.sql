-- ora数据库表结构
DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 第二个数据库表结构
DROP TABLE IF EXISTS products;

CREATE TABLE products (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          name VARCHAR(100) NOT NULL,
                          price DECIMAL(10, 2) NOT NULL,
                          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 第三个数据库表结构
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT NOT NULL,
                        product_id INT NOT NULL,
                        quantity INT NOT NULL,
                        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TEST1表结构
DROP TABLE IF EXISTS TEST1;

CREATE TABLE TEST1 (
  id INT AUTO_INCREMENT PRIMARY KEY,
  amount1 NUMBER(17,3),
  amount2 NUMBER(17,3),
  amount3 NUMBER(17,3),
  amount4 NUMBER(17,3),
  amount5 NUMBER(17,3),
  amount6 NUMBER(17,3),
  amount7 NUMBER(17,3),
  amount8 NUMBER(17,3),
  amount9 NUMBER(17,3),
  amount10 NUMBER(17,3)
); 