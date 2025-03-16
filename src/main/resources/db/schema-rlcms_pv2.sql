-- 创建订单表
CREATE TABLE IF NOT EXISTS ORDERS
(
    id           INT PRIMARY KEY,
    user_id      INT,
    order_date   DATE,
    total_amount DECIMAL(10, 2)
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