-- ora数据库数据
INSERT INTO users (username, email) VALUES 
('user1', 'user1@example.com'),
('user2', 'user2@example.com'),
('user3', 'user3@example.com'); 

-- TEST1表数据
INSERT INTO TEST1 (amount1, amount2, amount3, amount4, amount5, amount6, amount7, amount8, amount9, amount10) VALUES 
(1000.123, 2000.456, 3000.789, 4000.123, 5000.456, 6000.789, 7000.123, 8000.456, 9000.789, 10000.123),
(1500.321, 2500.654, 3500.987, 4500.321, 5500.654, 6500.987, 7500.321, 8500.654, 9500.987, 11000.321);

-- 公式1测试数据 (ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3)
INSERT INTO FORMULA1_TRUE (amount)
VALUES (300.00); -- 在rlcms_pv1, rlcms_pv2, rlcms_pv3分别插入100.00，确保和为300.00
INSERT INTO FORMULA1_FALSE (amount)
VALUES (301.00); -- 在rlcms_pv1, rlcms_pv2, rlcms_pv3分别插入100.00，确保和为300.00，但ora为301.00
INSERT INTO FORMULA1_NA (amount)
VALUES (300.00);
-- 不在所有rlcms数据源中创建此表，导致N/A结果

-- 公式2测试数据 (ora = rlcms_base)
INSERT INTO FORMULA2_TRUE (amount)
VALUES (200.00); -- 在rlcms_base插入200.00，确保和ora相等
INSERT INTO FORMULA2_FALSE (amount)
VALUES (200.00); -- 在rlcms_base插入201.00，确保和ora不相等
INSERT INTO FORMULA2_NA (amount)
VALUES (200.00);
-- 不在rlcms_base中创建此表，导致N/A结果

-- 公式3测试数据 (ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3)
INSERT INTO FORMULA3_TRUE (amount)
VALUES (500.00); -- 在所有数据源插入500.00，确保相等
INSERT INTO FORMULA3_FALSE (amount)
VALUES (500.00); -- 在某些数据源插入不同值，确保不相等
INSERT INTO FORMULA3_NA (amount)
VALUES (500.00);
-- 不在所有数据源中创建此表，导致N/A结果

-- 公式4测试数据 (ora = rlcms_pv1 = rlcms_pv2 = rlcms_pv3)
INSERT INTO FORMULA4_TRUE (amount)
VALUES (400.00); -- 在所有数据源插入400.00，确保相等
INSERT INTO FORMULA4_FALSE (amount)
VALUES (400.00); -- 在某些数据源插入不同值，确保不相等
INSERT INTO FORMULA4_NA (amount)
VALUES (400.00);
-- 不在所有数据源中创建此表，导致N/A结果

-- 公式5测试数据 (ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3)
INSERT INTO FORMULA5_TRUE (amount)
VALUES (600.00); -- 在所有数据源插入600.00，确保相等
INSERT INTO FORMULA5_FALSE (amount)
VALUES (600.00); -- 在某些数据源插入不同值，确保不相等
INSERT INTO FORMULA5_NA (amount)
VALUES (600.00);
-- 不在所有数据源中创建此表，导致N/A结果

-- 公式6测试数据 (ora = rlcms_pv1)
INSERT INTO FORMULA6_TRUE (amount)
VALUES (700.00); -- 在ora和rlcms_pv1插入700.00，确保相等
INSERT INTO FORMULA6_FALSE (amount)
VALUES (700.00); -- 在rlcms_pv1插入701.00，确保和ora不相等
INSERT INTO FORMULA6_NA (amount)
VALUES (700.00); -- 不在rlcms_pv1中创建此表，导致N/A结果