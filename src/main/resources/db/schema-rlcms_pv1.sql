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

-- 创建销售表
CREATE TABLE IF NOT EXISTS SALES
(
    id         INT PRIMARY KEY,
    product_id INT,
    quantity   INT,
    sale_date  DATE,
    amount     DECIMAL(10, 2)
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