-- 插入订单数据 (对于公式1/2，会有部分结果)
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (1, 1, '2025-01-01', 199.99);
INSERT INTO ORDERS (id, user_id, order_date, total_amount)
VALUES (3, 3, '2025-01-03', 399.99);

-- 插入销售数据 (对于公式2，会有FALSE结果)
INSERT INTO SALES (id, product_id, quantity, sale_date, amount)
VALUES (3, 3, 2, '2025-01-03', 199.98);

-- 插入分类数据 (与ora不同，对公式4会有FALSE结果)
INSERT INTO CATEGORY (id, name, description)
VALUES (1, '电子设备', '包括电脑、手机等');
INSERT INTO CATEGORY (id, name, description)
VALUES (2, '家居产品', '各类家居日用品');