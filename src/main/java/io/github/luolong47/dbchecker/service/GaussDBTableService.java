package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.StopWatch;
import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

@SuppressWarnings("ALL")
@Slf4j
@Service("gaussDBTableService")
public class GaussDBTableService extends AbstractTableService {

    @Override
    public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
        // 创建StopWatch来记录执行时间
        StopWatch watch = new StopWatch("GaussDB表信息查询");
        watch.start("GaussDB构建表查询SQL");
        
        // 将列表转换为IN子句的形式
        String schemasStr = schemas.stream().map(s -> "'" + s.toLowerCase() + "'").collect(Collectors.joining(","));
        String tablesStr = tables.stream().map(t -> "'" + t.toLowerCase() + "'").collect(Collectors.joining(","));

        String sql = "SELECT tablename AS TABLE_NAME, schemaname AS SCHEMA_NAME " +
                     "FROM pg_tables " +
                     "WHERE schemaname IN (" + schemasStr + ") " +
                     "AND tablename IN (" + tablesStr + ")";
        
        watch.stop();
        watch.start("GaussDB执行表查询SQL");
        log.debug("执行SQL: {}", sql);
        
        List<TableEnt> result = jdbcTemplate.query(sql, (rs, rowNum) -> {
            TableEnt tableEnt = new TableEnt();
            tableEnt.setTableName(rs.getString("TABLE_NAME"));
            tableEnt.setSchemaName(rs.getString("SCHEMA_NAME"));
            return tableEnt;
        });
        
        watch.stop();
        log.debug("GaussDB表信息查询执行完成，耗时统计：{}ms", watch.getTotalTimeMillis());
        
        return result;
    }

    @Override
    public Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits) {
        // 创建StopWatch来记录执行时间
        StopWatch watch = new StopWatch("GaussDB金额列查询");
        watch.start("GaussDB金额列查询前置检查");
        
        if (tables == null || tables.isEmpty()) {
            watch.stop();
            log.info("表列表为空，直接返回空结果");
            return Collections.emptyMap();
        }
        
        try {
            watch.stop();
            watch.start("GaussDB金额列查询构建SQL");
            
            // 将表名列表转换为IN子句形式
            String tablesStr = tables.stream()
                    .map(t -> "'" + t.toLowerCase() + "'")
                    .collect(Collectors.joining(","));
            schema = schema.toLowerCase();

            String sql = "SELECT table_name, column_name FROM information_schema.columns " +
                         "WHERE table_schema = ? " +
                         "AND table_name IN (" + tablesStr + ") " +
                         "AND data_type IN ('numeric', 'decimal') " +
                         "AND numeric_scale >= ? " +
                         "ORDER BY table_name, ordinal_position";
            
            watch.stop();
            watch.start("GaussDB金额列查询执行SQL");     
            log.debug("批量查询金额列，模式: {}, 最小小数位数: {}, SQL: {}", schema, minDecimalDigits, sql);
            
            // 使用Map来存储结果
            Map<String, List<String>> resultMap = new HashMap<>();
            
            jdbcTemplate.query(sql, (rs) -> {
                String tableName = rs.getString("table_name").toUpperCase();
                String columnName = rs.getString("column_name").toUpperCase();
                
                resultMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(columnName);
            }, schema, minDecimalDigits);
            
            watch.stop();
            log.debug("GaussDB金额列批量查询完成，共查询到 {} 个表的金额列信息，耗时统计：{}ms",
                resultMap.size(), watch.getTotalTimeMillis());
            return resultMap;
        } catch (Exception e) {
            watch.stop();
            log.error("GaussDB金额列批量查询时发生错误: {}, 耗时统计：{}ms",
                e.getMessage(), watch.getTotalTimeMillis(), e);
            return super.getDecimalColumnsForTables(jdbcTemplate, schema, tables, minDecimalDigits);
        }
    }

}
