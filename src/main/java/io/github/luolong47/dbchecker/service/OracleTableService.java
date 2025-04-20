package io.github.luolong47.dbchecker.service;

import cn.hutool.core.collection.ListUtil;
import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service("oracleTableService")
public class OracleTableService extends AbstractTableService {
    
    @Override
    public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
        // 将列表转换为IN子句的形式
        String schemasStr = schemas.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));

        List<List<String>> tablesSplit = ListUtil.split(tables, 1000);
        String tablesStr = tablesSplit.stream()
                .map(subList -> subList.stream()
                        .map(t -> "'" + t + "'")
                        .collect(Collectors.joining(",")))
                .collect(Collectors.joining(") OR TABLE_NAME IN ("));
        
        String sql = "SELECT TABLE_NAME, OWNER AS SCHEMA_NAME FROM ALL_TABLES " +
                     "WHERE OWNER IN (" + schemasStr + ") " +
                     "AND (TABLE_NAME IN (" + tablesStr + "))";
        
        log.info("执行SQL: {}", sql);
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            TableEnt tableEnt = new TableEnt();
            tableEnt.setTableName(rs.getString("TABLE_NAME"));
            tableEnt.setSchemaName(rs.getString("SCHEMA_NAME"));
            return tableEnt;
        });
    }

    @Override
    public Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits) {
        if (tables == null || tables.isEmpty()) {
            return Collections.emptyMap();
        }
        
        try {
            // 将表名列表转换为IN子句形式
            List<List<String>> tablesSplit = ListUtil.split(tables, 1000);
            String tablesStr = tablesSplit.stream()
                    .map(subList -> subList.stream()
                            .map(t -> "'" + t + "'")
                            .collect(Collectors.joining(",")))
                    .collect(Collectors.joining(") OR TABLE_NAME IN ("));
                    
            String sql = "SELECT TABLE_NAME, COLUMN_NAME FROM ALL_TAB_COLUMNS " +
                         "WHERE OWNER = ? " +
                         "AND (TABLE_NAME IN (" + tablesStr + ")) " +
                         "AND DATA_TYPE = 'NUMBER' " +
                         "AND DATA_SCALE >= ? " +
                         "ORDER BY TABLE_NAME, COLUMN_ID";
                         
            log.info("批量查询金额列，模式: {}, 最小小数位数: {}, SQL: {}", schema, minDecimalDigits, sql);
            
            // 使用Map来存储结果
            Map<String, List<String>> resultMap = new HashMap<>();
            
            jdbcTemplate.query(sql, (rs) -> {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                
                resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
            }, schema, minDecimalDigits);
            
            log.info("批量查询金额列完成，共查询到 {} 个表的金额列信息", resultMap.size());
            return resultMap;
        } catch (Exception e) {
            log.error("批量查询金额列时发生错误: {}", e.getMessage(), e);
            return super.getDecimalColumnsForTables(jdbcTemplate, schema, tables, minDecimalDigits);
        }
    }
} 
