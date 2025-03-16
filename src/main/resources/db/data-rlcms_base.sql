-- 插入产品数据
INSERT INTO PRODUCTS (id, name, price)
VALUES (1, '产品A', 199.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (2, '产品B', 299.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (3, '产品C', 99.99);

-- 插入订单数据 (与ora不同，以便产生FALSE结果)
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (1, 1, '2025-01-01', 199.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (2, 2, '2025-01-02', 299.99);

-- 插入用户数据 (与ora相同，以便产生TRUE结果)
INSERT INTO USERS (id, username, email, register_date)
VALUES (1, 'user1', 'user1@example.com', '2024-01-01');
INSERT INTO USERS (id, username, email, register_date)
VALUES (2, 'user2', 'user2@example.com', '2024-01-02');
INSERT INTO USERS (id, username, email, register_date)
VALUES (3, 'user3', 'user3@example.com', '2024-01-03');

-- 插入员工数据 (与ora相同，以便产生TRUE结果)
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (1, '员工A', '经理', 15000);
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (2, '员工B', '主管', 10000);
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (3, '员工C', '销售', 7000);

-- 插入库存数据 (与ora不同，以便产生N/A结果)
INSERT INTO INVENTORY (id, product_id, quantity, last_updated)
VALUES (1, 1, 90, '2025-01-01');
INSERT INTO INVENTORY (id, product_id, quantity, last_updated)
VALUES (2, 2, 45, '2025-01-01');