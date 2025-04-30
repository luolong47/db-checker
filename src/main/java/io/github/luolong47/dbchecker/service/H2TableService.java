package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.StopWatch;
import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;

@SuppressWarnings("ALL")
@Slf4j
@Service("h2TableService")
public class H2TableService extends AbstractTableService  {
    
    @Override
    public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
        // 创建StopWatch来记录执行时间
        StopWatch watch = new StopWatch("H2表信息查询");
        watch.start("H2构建表查询SQL");
        
        // 将列表转换为IN子句的形式
        String schemasStr = schemas.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        String tablesStr = tables.stream().map(t -> "'" + t + "'").collect(Collectors.joining(","));

        String sql = "SELECT TABLE_NAME, TABLE_SCHEMA AS SCHEMA_NAME " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_SCHEMA IN (" + schemasStr + ") " +
            "AND TABLE_NAME IN (" + tablesStr + ")";
        
        watch.stop();
        watch.start("H2执行表查询SQL");
        log.debug("执行SQL: {}", sql);
        
        List<TableEnt> result = jdbcTemplate.query(sql, (rs, rowNum) -> {
            TableEnt tableEnt = new TableEnt();
            tableEnt.setTableName(rs.getString("TABLE_NAME"));
            tableEnt.setSchemaName(rs.getString("SCHEMA_NAME"));
            return tableEnt;
        });
        
        watch.stop();
        log.debug("H2表信息查询执行完成，耗时统计：{}ms", watch.getTotalTimeMillis());
        
        return result;
    }

    @Override
    public Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits) {
        // 创建StopWatch来记录执行时间
        StopWatch watch = new StopWatch("H2金额列查询");
        watch.start("H2金额列查询前置检查");
        
        if (tables == null || tables.isEmpty()) {
            watch.stop();
            log.info("表列表为空，直接返回空结果");
            return Collections.emptyMap();
        }
        
        try {
            watch.stop();
            watch.start("H2金额列查询构建SQL");
            
            // 将表名列表转换为IN子句形式
            String tablesStr = tables.stream()
                    .map(t -> "'" + t + "'")
                    .collect(Collectors.joining(","));
                    
            String sql = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                         "WHERE TABLE_SCHEMA = ? " +
                         "AND TABLE_NAME IN (" + tablesStr + ") " +
                         "AND DATA_TYPE IN ('DECIMAL', 'NUMERIC') " +
                         "AND NUMERIC_SCALE >= ? " +
                         "ORDER BY TABLE_NAME, ORDINAL_POSITION";
            
            watch.stop();
            watch.start("H2金额列查询执行SQL");     
            log.debug("批量查询金额列，模式: {}, 最小小数位数: {}, SQL: {}", schema, minDecimalDigits, sql);
            
            // 使用Map来存储结果
            Map<String, List<String>> resultMap = new HashMap<>();
            
            jdbcTemplate.query(sql, (rs) -> {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                
                resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
            }, schema, minDecimalDigits);
            
            watch.stop();
            log.debug("H2金额列批量查询完成，共查询到 {} 个表的金额列信息，耗时统计：{}ms",
                resultMap.size(), watch.getTotalTimeMillis());
            return resultMap;
        } catch (Exception e) {
            watch.stop();
            log.error("H2金额列批量查询时发生错误: {}, 耗时统计：{}ms",
                e.getMessage(), watch.getTotalTimeMillis(), e);
            return super.getDecimalColumnsForTables(jdbcTemplate, schema, tables, minDecimalDigits);
        }
    }
}
