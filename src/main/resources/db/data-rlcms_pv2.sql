-- 插入订单数据 (对于公式1/2，会有部分结果)
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (1, 1, '2025-01-01', 199.99);

-- 插入支付数据 (对于公式4，会有FALSE结果)
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (1, 1, '2025-01-01', 199.99, '支付宝');
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (2, 2, '2025-01-02', 399.99, '微信支付');