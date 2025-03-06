-- bscopy_pv3数据库表结构
DROP TABLE IF EXISTS logs;

CREATE TABLE logs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  level VARCHAR(10) NOT NULL,
  logger VARCHAR(255) NOT NULL,
  message TEXT,
  exception TEXT,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  thread VARCHAR(100),
  user_id INT,
  ip_address VARCHAR(50),
  request_id VARCHAR(100)
);

-- 系统配置表结构
DROP TABLE IF EXISTS system_config;

CREATE TABLE system_config (
  id INT AUTO_INCREMENT PRIMARY KEY,
  config_key VARCHAR(100) NOT NULL UNIQUE,
  config_value TEXT,
  description VARCHAR(255),
  type VARCHAR(20),
  modified_by VARCHAR(50),
  modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  active BOOLEAN DEFAULT TRUE
);

-- TEST8表结构
DROP TABLE IF EXISTS TEST8;

CREATE TABLE TEST8 (
  id INT AUTO_INCREMENT PRIMARY KEY,
  metric_name VARCHAR(100),
  metric_value DECIMAL(15,5),
  collect_time TIMESTAMP,
  source VARCHAR(50),
  tag1 VARCHAR(50),
  tag2 VARCHAR(50),
  tag3 VARCHAR(50),
  note TEXT,
  is_valid BOOLEAN,
  duration_ms BIGINT
); 