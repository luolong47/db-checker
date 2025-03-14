-- bscopy_pv3数据库数据
INSERT INTO logs (level, logger, message, exception, thread, user_id, ip_address, request_id) VALUES 
('INFO', 'com.example.Controller', '用户登录成功', NULL, 'http-nio-8080-exec-1', 1, '192.168.1.100', 'REQ-001'),
('ERROR', 'com.example.Service', '数据库连接失败', 'java.sql.SQLException: Connection refused', 'http-nio-8080-exec-2', NULL, '192.168.1.101', 'REQ-002'),
('WARN', 'com.example.Security', '尝试访问受限资源', NULL, 'http-nio-8080-exec-3', 2, '192.168.1.102', 'REQ-003');

-- 系统配置表数据
INSERT INTO system_config (config_key, config_value, description, type, modified_by, active) VALUES 
('app.theme', 'dark', '应用主题设置', 'STRING', 'admin', true),
('app.timeout', '3600', '会话超时时间（秒）', 'INTEGER', 'admin', true),
('app.features.new_dashboard', 'true', '是否启用新版仪表盘', 'BOOLEAN', 'admin', true),
('app.notification.email', 'true', '是否启用邮件通知', 'BOOLEAN', 'admin', true),
('app.api.rate_limit', '100', 'API调用频率限制（次/分钟）', 'INTEGER', 'admin', true);

-- TEST8表数据
INSERT INTO TEST8 (metric_name, metric_value, collect_time, source, tag1, tag2, tag3, note, is_valid, duration_ms) VALUES 
('cpu.usage', 75.5, CURRENT_TIMESTAMP, 'server1', 'prod', 'webapp', 'instance1', 'CPU使用率峰值', true, 50),
('memory.usage', 60.2, CURRENT_TIMESTAMP, 'server1', 'prod', 'webapp', 'instance1', '内存使用情况正常', true, 45),
('disk.free', 25.8, CURRENT_TIMESTAMP, 'server2', 'prod', 'database', 'instance1', '磁盘空间不足警告', true, 60),
('network.latency', 120.5, CURRENT_TIMESTAMP, 'server3', 'prod', 'api', 'instance2', '网络延迟增加', true, 48);

-- 公式3测试数据
INSERT INTO FORMULA3_TRUE (amount)
VALUES (500.00); -- 与ora一致
INSERT INTO FORMULA3_FALSE (amount)
VALUES (504.00); -- 与ora不一致