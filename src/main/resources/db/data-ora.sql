-- 插入产品数据
INSERT INTO PRODUCTS (id, name, price)
VALUES (1, '产品A', 199.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (2, '产品B', 299.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (3, '产品C', 99.99);

-- 插入订单数据
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (1, 1, '2025-01-01', 199.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (2, 2, '2025-01-02', 299.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (3, 3, '2025-01-03', 399.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (4, 1, '2025-01-04', 499.99);

-- 插入用户数据
INSERT INTO USERS (id, username, email, register_date)
VALUES (1, 'user1', 'user1@example.com', '2024-01-01');
INSERT INTO USERS (id, username, email, register_date)
VALUES (2, 'user2', 'user2@example.com', '2024-01-02');
INSERT INTO USERS (id, username, email, register_date)
VALUES (3, 'user3', 'user3@example.com', '2024-01-03');

-- 插入客户数据
INSERT INTO CUSTOMERS (id, name, contact, address)
VALUES (1, '客户A', '13800001111', '北京市');
INSERT INTO CUSTOMERS (id, name, contact, address)
VALUES (2, '客户B', '13800002222', '上海市');
INSERT INTO CUSTOMERS (id, name, contact, address)
VALUES (3, '客户C', '13800003333', '广州市');

-- 插入员工数据
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (1, '员工A', '经理', 15000);
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (2, '员工B', '主管', 10000);
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (3, '员工C', '销售', 7000);

-- 插入销售数据
INSERT INTO SALES (id, product_id, quantity, sale_date, amount)
VALUES (1, 1, 2, '2025-01-01', 399.98);
INSERT INTO SALES (id, product_id, quantity, sale_date, amount)
VALUES (2, 2, 1, '2025-01-02', 299.99);
INSERT INTO SALES (id, product_id, quantity, sale_date, amount)
VALUES (3, 3, 3, '2025-01-03', 299.97);

-- 插入库存数据
INSERT INTO INVENTORY (id, product_id, quantity, last_updated)
VALUES (1, 1, 100, '2025-01-01');
INSERT INTO INVENTORY (id, product_id, quantity, last_updated)
VALUES (2, 2, 50, '2025-01-01');
INSERT INTO INVENTORY (id, product_id, quantity, last_updated)
VALUES (3, 3, 200, '2025-01-01');

-- 插入供应商数据
INSERT INTO SUPPLIERS (id, name, contact, address)
VALUES (1, '供应商A', '13900001111', '深圳市');
INSERT INTO SUPPLIERS (id, name, contact, address)
VALUES (2, '供应商B', '13900002222', '杭州市');
INSERT INTO SUPPLIERS (id, name, contact, address)
VALUES (3, '供应商C', '13900003333', '成都市');

-- 插入分类数据
INSERT INTO CATEGORY (id, name, description)
VALUES (1, '电子产品', '包括电脑、手机等电子设备');
INSERT INTO CATEGORY (id, name, description)
VALUES (2, '家居用品', '各类家居日用品');
INSERT INTO CATEGORY (id, name, description)
VALUES (3, '食品', '各类食品和饮料');

-- 插入支付数据
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (1, 1, '2025-01-01', 199.99, '支付宝');
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (2, 2, '2025-01-02', 299.99, '微信支付');
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (3, 3, '2025-01-03', 399.99, '银行卡');
INSERT INTO PAYMENTS (id, order_id, payment_date, amount, method)
VALUES (4, 4, '2025-01-04', 499.99, '支付宝');