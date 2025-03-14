-- 插入用户数据 (与ora相同，对公式3会有TRUE结果)
INSERT INTO USERS (id, username, email, register_date)
VALUES (1, 'user1', 'user1@example.com', '2024-01-01');
INSERT INTO USERS (id, username, email, register_date)
VALUES (2, 'user2', 'user2@example.com', '2024-01-02');
INSERT INTO USERS (id, username, email, register_date)
VALUES (3, 'user3', 'user3@example.com', '2024-01-03');

-- 插入员工数据 (与ora相同，对公式3会有TRUE结果)
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (1, '员工A', '经理', 15000);
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (2, '员工B', '主管', 10000);
INSERT INTO EMPLOYEES (id, name, position, salary)
VALUES (3, '员工C', '销售', 7000);