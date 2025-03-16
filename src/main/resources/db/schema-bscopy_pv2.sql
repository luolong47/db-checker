-- 创建产品表
CREATE TABLE IF NOT EXISTS PRODUCTS
(
    id    INT PRIMARY KEY,
    name  VARCHAR(100),
    price DECIMAL(10, 2)
);

-- 创建用户表
CREATE TABLE IF NOT EXISTS USERS
(
    id            INT PRIMARY KEY,
    username      VARCHAR(50),
    email         VARCHAR(100),
    register_date DATE
);

-- 创建供应商表
CREATE TABLE IF NOT EXISTS SUPPLIERS
(
    id      INT PRIMARY KEY,
    name    VARCHAR(100),
    contact VARCHAR(100),
    address VARCHAR(255)
); 