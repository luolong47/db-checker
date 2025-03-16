-- 插入产品数据 (对于公式6，会有TRUE结果)
INSERT INTO PRODUCTS (id, name, price)
VALUES (1, '产品A', 199.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (2, '产品B', 299.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (3, '产品C', 99.99);

-- 插入订单数据 (对于公式1，会有部分结果)
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (1, 1, '2025-01-01', 199.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (2, 2, '2025-01-02', 299.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (3, 3, '2025-01-03', 399.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (4, 1, '2025-01-04', 499.99);

-- 插入用户数据 (对于公式6，会有TRUE结果)
INSERT INTO USERS (id, username, email, register_date)
VALUES (1, 'user1', 'user1@example.com', '2024-01-01');
INSERT INTO USERS (id, username, email, register_date)
VALUES (2, 'user2', 'user2@example.com', '2024-01-02');
INSERT INTO USERS (id, username, email, register_date)
VALUES (3, 'user3', 'user3@example.com', '2024-01-03');

-- 插入客户数据 (对于公式4，会有N/A结果，因为数据与ora不同)
INSERT INTO CUSTOMERS (id, name, contact, address)
VALUES (1, '客户A', '13800001111', '北京市');
INSERT INTO CUSTOMERS (id, name, contact, address)
VALUES (2, '客户B', '13800002222', '上海市');

-- 插入销售数据 (对于公式2，会有FALSE结果)
INSERT INTO SALES (id, product_id, quantity, sale_date, amount)
VALUES (1, 1, 1, '2025-01-01', 199.99);
INSERT INTO SALES (id, product_id, quantity, sale_date, amount)
VALUES (2, 2, 1, '2025-01-02', 299.99);

-- 插入支付数据 (对于公式4，会有FALSE结果)
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (1, 1, '2025-01-01', 199.99, '支付宝');
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (2, 2, '2025-01-02', 299.99, '微信支付');