-- 第五个数据库数据
INSERT INTO departments (name, location, budget) VALUES 
('研发部', '北京', 1000000.00),
('市场部', '上海', 800000.00),
('财务部', '深圳', 500000.00);

-- 员工表数据
INSERT INTO employees (department_id, first_name, last_name, position, salary, hire_date) VALUES 
(1, '赵', '一', '软件工程师', 15000.00, '2020-05-10'),
(1, '钱', '二', '产品经理', 18000.00, '2019-08-15'),
(2, '孙', '三', '市场经理', 16000.00, '2021-03-20'),
(3, '李', '四', '财务主管', 17000.00, '2018-11-05');

-- TEST5表数据
INSERT INTO TEST5 (code, name, category, active, price, discount, stock, min_stock, supplier_id, last_order_date) VALUES 
('SKU001', '商品A', '电子', true, 299.99, 10.00, 50, 10, 1, '2023-09-15'),
('SKU002', '商品B', '家居', true, 199.50, 5.00, 100, 20, 2, '2023-10-20'),
('SKU003', '商品C', '食品', false, 99.99, 15.00, 30, 5, 1, '2023-08-10'); 