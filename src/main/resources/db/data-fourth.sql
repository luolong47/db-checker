-- 第四个数据库数据
INSERT INTO customers (first_name, last_name, phone, email, address) VALUES 
('张', '三', '13800138001', 'zhangsan@example.com', '北京市海淀区'),
('李', '四', '13900139002', 'lisi@example.com', '上海市浦东新区'),
('王', '五', '13700137003', 'wangwu@example.com', '广州市天河区');

-- 订阅表数据
INSERT INTO subscriptions (customer_id, plan_name, start_date, end_date, status) VALUES 
(1, '基础套餐', '2023-01-01', '2023-12-31', '活跃'),
(2, '高级套餐', '2023-02-15', '2024-02-14', '活跃'),
(3, '专业套餐', '2023-03-10', '2023-09-10', '已过期');

-- TEST4表数据
INSERT INTO TEST4 (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10) VALUES 
('测试1', 123.45, CURRENT_TIMESTAMP, true, '描述1', 100, 10.4321, 'A1', '2023-01-15', '详细描述1'),
('测试2', 678.90, CURRENT_TIMESTAMP, false, '描述2', 200, 20.8642, 'B2', '2023-02-20', '详细描述2'); 