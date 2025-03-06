-- bscopy_pv2数据库数据
INSERT INTO warehouses (name, location, capacity, manager_id, contact_phone, active) VALUES 
('北京仓库', '北京市朝阳区', 10000, 1, '010-12345678', true),
('上海仓库', '上海市浦东新区', 15000, 2, '021-12345678', true),
('广州仓库', '广州市天河区', 8000, 3, '020-12345678', true);

-- 库存表数据
INSERT INTO inventory (sku, product_name, category, quantity, unit_price, supplier_id, warehouse_id, reorder_level) VALUES 
('SKU10001', '笔记本电脑', '电子产品', 50, 5999.00, 1, 1, 10),
('SKU10002', '智能手机', '电子产品', 100, 3999.00, 2, 1, 20),
('SKU10003', '办公桌', '办公家具', 30, 1299.00, 3, 2, 5),
('SKU10004', '办公椅', '办公家具', 50, 699.00, 3, 2, 10),
('SKU10005', '打印机', '办公设备', 20, 1599.00, 1, 3, 5);

-- TEST7表数据
INSERT INTO TEST7 (transaction_id, transaction_date, customer_id, amount, payment_method, status, notes, processed_by, reference_no, batch_id) VALUES 
('TRX001', CURRENT_TIMESTAMP, 1, 1299.99, '支付宝', '已完成', '首次购买', 101, 'REF20230001', 'B202301'),
('TRX002', CURRENT_TIMESTAMP, 2, 2599.98, '微信支付', '已完成', '促销活动购买', 102, 'REF20230002', 'B202301'),
('TRX003', CURRENT_TIMESTAMP, 3, 899.50, '银行卡', '处理中', '需要验证', 103, 'REF20230003', 'B202302'); 