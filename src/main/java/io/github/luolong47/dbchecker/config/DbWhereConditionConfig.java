package io.github.luolong47.dbchecker.config;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库查询WHERE条件配置
 */
@Data
@Slf4j
@ConfigurationProperties(prefix = "db")
public class DbWhereConditionConfig {
    
    private Map<String, Map<String, String>> where = new HashMap<>();
    
    /**
     * 初始化时记录已加载的条件
     */
    @PostConstruct
    public void init() {
        if (where == null || where.isEmpty()) {
            log.warn("未加载任何db.where条件配置");
        } else {
            log.info("已加载{}个数据源的条件配置:", where.size());
            for (Map.Entry<String, Map<String, String>> entry : where.entrySet()) {
                String dataSource = entry.getKey();
                Map<String, String> tableConditions = entry.getValue();
                log.info("数据源[{}]条件配置: {}", dataSource, tableConditions);
            }
        }
    }
    
    /**
     * 获取指定数据源和表的WHERE条件
     * 
     * @param dataSourceName 数据源名称
     * @param tableName 表名
     * @return WHERE条件（不含WHERE关键字）或null
     */
    public String getCondition(String dataSourceName, String tableName) {
        // 忽略大小写查找数据源
        Map<String, String> tableConditions = null;
        for (Map.Entry<String, Map<String, String>> entry : where.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(dataSourceName)) {
                tableConditions = entry.getValue();
                log.debug("找到数据源 [{}] 的条件配置", dataSourceName);
                break;
            }
        }
        
        if (tableConditions != null) {
            // 忽略大小写查找表名
            for (Map.Entry<String, String> entry : tableConditions.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(tableName)) {
                    log.debug("找到表 [{}] 的条件: {}", tableName, entry.getValue());
                    return entry.getValue();
                }
            }
            log.debug("未找到表 [{}] 的条件配置", tableName);
        } else {
            log.debug("未找到数据源 [{}] 的条件配置", dataSourceName);
        }
        return null;
    }
    
    /**
     * 应用WHERE条件到SQL语句
     * 
     * @param sql 原始SQL
     * @param dataSourceName 数据源名称
     * @param tableName 表名
     * @return 添加了WHERE条件的SQL
     */
    public String applyCondition(String sql, String dataSourceName, String tableName) {
        String condition = getCondition(dataSourceName, tableName);
        if (condition != null && !condition.isEmpty()) {
            String newSql;
            if (sql.toLowerCase().contains(" where ")) {
                newSql = sql + " AND " + condition;
            } else {
                newSql = sql + " WHERE " + condition;
            }
            log.info("应用WHERE条件 - 原SQL: [{}], 新SQL: [{}]", sql, newSql);
            return newSql;
        }
        return sql;
    }
} 