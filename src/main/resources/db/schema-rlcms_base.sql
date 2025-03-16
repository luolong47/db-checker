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

-- 创建员工表
CREATE TABLE IF NOT EXISTS EMPLOYEES
(
    id       INT PRIMARY KEY,
    name     VARCHAR(100),
    position VARCHAR(100),
    salary   DECIMAL(10, 2)
);

-- 创建库存表
CREATE TABLE IF NOT EXISTS INVENTORY
(
    id           INT PRIMARY KEY,
    product_id   INT,
    quantity     INT,
    last_updated DATE
); 