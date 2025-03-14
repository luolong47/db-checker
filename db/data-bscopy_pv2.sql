-- 插入产品数据 (与ora相同，对公式3会有TRUE结果)
INSERT INTO PRODUCTS (id, name, price)
VALUES (1, '产品A', 199.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (2, '产品B', 299.99);
INSERT INTO PRODUCTS (id, name, price)
VALUES (3, '产品C', 99.99);

-- 插入用户数据 (与ora相同，对公式3会有TRUE结果)
INSERT INTO USERS (id, username, email, register_date)
VALUES (1, 'user1', 'user1@example.com', '2024-01-01');
INSERT INTO USERS (id, username, email, register_date)
VALUES (2, 'user2', 'user2@example.com', '2024-01-02');
INSERT INTO USERS (id, username, email, register_date)
VALUES (3, 'user3', 'user3@example.com', '2024-01-03');

-- 插入供应商数据 (与ora相同，用于公式1会有TRUE结果)
INSERT INTO SUPPLIERS (id, name, contact, address)
VALUES (1, '供应商A', '13900001111', '深圳市');
INSERT INTO SUPPLIERS (id, name, contact, address)
VALUES (2, '供应商B', '13900002222', '杭州市');
INSERT INTO SUPPLIERS (id, name, contact, address)
VALUES (3, '供应商C', '13900003333', '成都市');