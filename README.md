# 多数据源查询测试工具

这个应用程序演示了如何使用Spring Boot配置和管理多个HikariCP数据源。

## 功能特点

1. 支持8个独立配置的H2数据库数据源
2. 全局Hikari配置和数据源特定配置
3. 基于JdbcTemplate的数据库操作
4. 动态数据源管理和切换
5. 数据源健康检查

## 运行说明

### 1. 启动应用程序

```bash
./mvnw spring-boot:run
```

### 2. 运行内置测试

在`application.yml`中设置:

```yaml
db:
  test:
    enabled: true
```

或通过命令行启用测试:

```bash
./mvnw spring-boot:run -Ddb.test.enabled=true
```

### 3. 执行自定义SQL

```bash
./mvnw spring-boot:run -Ddb.custom-execute.enabled=true -Ddb.custom-execute.sql="SELECT * FROM USERS WHERE id > 10"
```

指定数据源:

```bash
./mvnw spring-boot:run -Ddb.custom-execute.enabled=true -Ddb.custom-execute.datasources=ora,rlcms-pv1 -Ddb.custom-execute.sql="SELECT COUNT(*) FROM ORDERS"
```

## 配置说明

### 数据源配置

配置文件结构采用层次化设计，全局Hikari配置位于`spring.datasource.hikari`节点，各数据源配置位于`spring.datasource.sources`节点下：

```yaml
spring:
  datasource:
    hikari:
      maximum-pool-size: 32
      minimum-idle: 8
      # ...其他Hikari全局配置
    
    sources:
      ora:
        driver-class-name: org.h2.Driver
        jdbc-url: jdbc:h2:file:./data/ora;AUTO_SERVER=TRUE;
        username: sa
        password:
        hikari:
          pool-name: OraPool
          
      # ...其他数据源配置
```

数据源特定的Hikari配置会覆盖全局配置，便于针对不同数据源特点优化连接池参数。

### 测试配置

在`application.yml`中配置测试相关参数:

```yaml
db:
  test:
    enabled: true  # 启用默认测试
  
  custom-execute:
    enabled: false # 是否启用自定义SQL执行
    sql: "SELECT * FROM USERS LIMIT 10"  # 默认执行SQL
    datasources: ora,rlcms-base  # 默认数据源列表
``` 