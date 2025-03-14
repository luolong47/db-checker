-- 创建订单表
CREATE TABLE IF NOT EXISTS ORDERS
(
    id           INT PRIMARY KEY,
    user_id      INT,
    order_date   DATE,
    total_amount DECIMAL(10, 2)
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

-- 创建分类表
CREATE TABLE IF NOT EXISTS CATEGORY
(
    id          INT PRIMARY KEY,
    name        VARCHAR(50),
    description VARCHAR(255)
); 