package io.github.luolong47.dbchecker.service;

import cn.hutool.extra.spring.SpringUtil;
import com.zaxxer.hikari.HikariDataSource;
import io.github.luolong47.dbchecker.entity.TableEnt;
import io.github.luolong47.dbchecker.manager.DynamicDataSourceManager;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public interface TableService {
    
    Logger log = LoggerFactory.getLogger(TableService.class);

    static TableService getTableService(String dbName){
        DynamicDataSourceManager dataSourceManager = SpringUtil.getBean(DynamicDataSourceManager.class);
        
        // 检查数据源是否存在
        if (!dataSourceManager.hasDataSource(dbName)) {
            log.warn("数据源 [{}] 不存在或已禁用，返回空TableService", dbName);
            return new DisabledTableService(dbName);
        }
        
        DataSource dataSource = dataSourceManager.getDataSource(dbName);
        
        // 处理不同类型的数据源
        if (dataSource instanceof HikariDataSource) {
            String driverClassName = ((HikariDataSource) dataSource).getDriverClassName();
            // 根据驱动类名判断数据库类型并返回对应的TableService
            if (driverClassName.contains("oracle")) {
                return SpringUtil.getBean("oracleTableService", TableService.class);
            } else if (driverClassName.contains("h2")) {
                return SpringUtil.getBean("h2TableService", TableService.class);
            } else if (driverClassName.contains("postgresql") || driverClassName.contains("gaussdb")) {
                return SpringUtil.getBean("gaussDBTableService", TableService.class);
            } else {
                throw new RuntimeException("不支持的数据库类型: " + driverClassName);
            }
        } else {
            // 处理禁用的数据源
            log.warn("数据源 [{}] 已禁用或类型未知，返回空TableService", dbName);
            return new DisabledTableService(dbName);
        }
    }

    List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables);

    /**
     * 批量获取多个表的金额字段
     * 
     * @param jdbcTemplate     JDBC模板
     * @param schema           模式名
     * @param tables           表名列表
     * @param minDecimalDigits 最小小数位数
     * @return 表名到金额字段列表的映射
     */
    Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits);
    
    /**
     * 禁用的TableService实现
     * 所有方法返回空结果
     */
    @Slf4j
    class DisabledTableService implements TableService {
        private final String dataSourceName;
        
        public DisabledTableService(String dataSourceName) {
            this.dataSourceName = dataSourceName;
            log.info("创建禁用的TableService, 数据源: [{}]", dataSourceName);
        }
        
        @Override
        public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
            log.warn("尝试从禁用的数据源 [{}] 获取表信息，返回空列表", dataSourceName);
            return Collections.emptyList();
        }
        
        @Override
        public Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits) {
            log.warn("尝试从禁用的数据源 [{}] 获取小数列信息，返回空映射", dataSourceName);
            return Collections.emptyMap();
        }
    }
}
