package io.github.luolong47.dbchecker.service;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.StopWatch;
import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("ALL")
@Slf4j
@Service("oracleTableService")
public class OracleTableService extends AbstractTableService {
    
    @Override
    public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
        // 创建StopWatch来记录执行时间
        StopWatch watch = new StopWatch("Oracle表信息查询");
        watch.start("Oracle构建表查询SQL");
        
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
        
        watch.stop();
        watch.start("Oracle执行表查询SQL");
        log.debug("执行SQL: {}", sql);
        
        List<TableEnt> result = jdbcTemplate.query(sql, (rs, rowNum) -> {
            TableEnt tableEnt = new TableEnt();
            tableEnt.setTableName(rs.getString("TABLE_NAME"));
            tableEnt.setSchemaName(rs.getString("SCHEMA_NAME"));
            return tableEnt;
        });
        
        watch.stop();
        log.info("Oracle表信息查询执行完成，耗时统计：{}ms", watch.getTotalTimeMillis());
        
        return result;
    }

    @Override
    public Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits) {
        // 创建StopWatch来记录执行时间
        StopWatch watch = new StopWatch("Oracle金额列查询");
        watch.start("Oracle金额列查询前置检查");
        
        if (tables == null || tables.isEmpty()) {
            watch.stop();
            log.warn("表列表为空，直接返回空结果");
            return Collections.emptyMap();
        }
        
        try {
            watch.stop();
            watch.start("Oracle金额列查询构建SQL");
            
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
            
            watch.stop();
            watch.start("Oracle金额列查询执行SQL");     
            log.debug("批量查询金额列，模式: {}, 最小小数位数: {}, SQL: {}", schema, minDecimalDigits, sql);
            
            // 使用Map来存储结果
            Map<String, List<String>> resultMap = new HashMap<>();
            
            jdbcTemplate.query(sql, (rs) -> {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                
                resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
            }, schema, minDecimalDigits);
            
            watch.stop();
            log.debug("Oracle金额列批量查询完成，共查询到 {} 个表的金额列信息，耗时统计：{}ms",
                resultMap.size(), watch.getTotalTimeMillis());
            return resultMap;
        } catch (Exception e) {
            watch.stop();
            log.error("Oracle金额列批量查询时发生错误: {}, 耗时统计：{}",
                e.getMessage(), watch.getTotalTimeMillis(), e);
            return super.getDecimalColumnsForTables(jdbcTemplate, schema, tables, minDecimalDigits);
        }
    }
} 
