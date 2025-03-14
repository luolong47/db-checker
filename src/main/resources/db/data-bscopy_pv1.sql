-- bscopy_pv1数据库数据
INSERT INTO projects (name, description, start_date, end_date, budget, status) VALUES 
('项目A', '这是项目A的描述', '2023-01-10', '2023-06-30', 500000.00, '进行中'),
('项目B', '这是项目B的描述', '2023-03-15', '2023-12-31', 800000.00, '进行中'),
('项目C', '这是项目C的描述', '2022-11-01', '2023-04-30', 300000.00, '已完成');

-- 项目任务表数据
INSERT INTO tasks (project_id, name, description, assigned_to, due_date, priority, status) VALUES 
(1, '需求分析', '完成项目A的需求分析文档', 1, '2023-01-20', '高', '已完成'),
(1, '系统设计', '完成项目A的系统设计文档', 2, '2023-02-15', '高', '已完成'),
(1, '编码实现', '完成项目A的核心功能开发', 3, '2023-04-30', '中', '进行中'),
(2, '需求收集', '收集项目B的用户需求', 4, '2023-03-30', '高', '已完成'),
(2, '原型设计', '设计项目B的用户界面原型', 5, '2023-04-15', '中', '进行中');

-- TEST6表数据
INSERT INTO TEST6 (reference_code, title, description, created_by, created_date, modified_by, modified_date, status, priority, category) VALUES 
('REF001', '标题1', '描述内容1', 1, CURRENT_TIMESTAMP, 1, CURRENT_TIMESTAMP, '活跃', 1, '类别A'),
('REF002', '标题2', '描述内容2', 2, CURRENT_TIMESTAMP, 2, CURRENT_TIMESTAMP, '待审核', 2, '类别B'),
('REF003', '标题3', '描述内容3', 3, CURRENT_TIMESTAMP, 3, CURRENT_TIMESTAMP, '已归档', 3, '类别C');

-- 公式3测试数据
INSERT INTO FORMULA3_TRUE (amount)
VALUES (500.00); -- 与ora一致
INSERT INTO FORMULA3_FALSE (amount)
VALUES (501.00); -- 与ora不一致