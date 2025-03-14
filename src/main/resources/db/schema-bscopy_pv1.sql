-- bscopy_pv1数据库表结构
DROP TABLE IF EXISTS projects;

CREATE TABLE projects (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  start_date DATE,
  end_date DATE,
  budget DECIMAL(15,2),
  status VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 项目任务表结构
DROP TABLE IF EXISTS tasks;

CREATE TABLE tasks (
  id INT AUTO_INCREMENT PRIMARY KEY,
  project_id INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  assigned_to INT,
  due_date DATE,
  priority VARCHAR(10),
  status VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TEST6表结构
DROP TABLE IF EXISTS TEST6;

CREATE TABLE TEST6 (
  id INT AUTO_INCREMENT PRIMARY KEY,
  reference_code VARCHAR(30),
  title VARCHAR(200),
  description TEXT,
  created_by INT,
  created_date TIMESTAMP,
  modified_by INT,
  modified_date TIMESTAMP,
  status VARCHAR(20),
  priority INT,
  category VARCHAR(50)
);

-- 公式3测试表
DROP TABLE IF EXISTS FORMULA3_TRUE;
CREATE TABLE FORMULA3_TRUE
(
    id     INT AUTO_INCREMENT PRIMARY KEY,
    amount DECIMAL(10, 2) NOT NULL
);

DROP TABLE IF EXISTS FORMULA3_FALSE;
CREATE TABLE FORMULA3_FALSE
(
    id     INT AUTO_INCREMENT PRIMARY KEY,
    amount DECIMAL(10, 2) NOT NULL
); 