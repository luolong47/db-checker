-- 创建产品表
CREATE TABLE IF NOT EXISTS PRODUCTS
(
    id    INT PRIMARY KEY,
    name  VARCHAR(100),
    price DECIMAL(10, 2)
);

-- 创建订单表
CREATE TABLE IF NOT EXISTS ORDERS
(
    id           INT PRIMARY KEY,
    user_id      INT,
    order_date   DATE,
    total_amount DECIMAL(10, 2)
);

-- 创建用户表
CREATE TABLE IF NOT EXISTS USERS
(
    id            INT PRIMARY KEY,
    username      VARCHAR(50),
    email         VARCHAR(100),
    register_date DATE
);

-- 创建客户表
CREATE TABLE IF NOT EXISTS CUSTOMERS
(
    id      INT PRIMARY KEY,
    name    VARCHAR(100),
    contact VARCHAR(100),
    address VARCHAR(255)
);

-- 创建员工表
CREATE TABLE IF NOT EXISTS EMPLOYEES
(
    id       INT PRIMARY KEY,
    name     VARCHAR(100),
    position VARCHAR(100),
    salary   DECIMAL(10, 2)
);

-- 创建销售表
CREATE TABLE IF NOT EXISTS SALES
(
    id         INT PRIMARY KEY,
    product_id INT,
    quantity   INT,
    sale_date  DATE,
    amount     DECIMAL(10, 2)
);

-- 创建库存表
CREATE TABLE IF NOT EXISTS INVENTORY
(
    id           INT PRIMARY KEY,
    product_id   INT,
    quantity     INT,
    last_updated DATE
);

-- 创建供应商表
CREATE TABLE IF NOT EXISTS SUPPLIERS
(
    id      INT PRIMARY KEY,
    name    VARCHAR(100),
    contact VARCHAR(100),
    address VARCHAR(255)
);

-- 创建分类表
CREATE TABLE IF NOT EXISTS CATEGORY
(
    id          INT PRIMARY KEY,
    name        VARCHAR(50),
    description VARCHAR(255)
);

-- 创建支付表
CREATE TABLE IF NOT EXISTS PAYMENTS
(
    id           INT PRIMARY KEY,
    order_id     INT,
    payment_date DATE,
    amount       DECIMAL(10, 2),
    method       VARCHAR(50)
); 