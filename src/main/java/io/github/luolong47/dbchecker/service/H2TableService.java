package io.github.luolong47.dbchecker.service;

import cn.hutool.core.collection.ListUtil;
import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("h2TableService")
public class H2TableService extends AbstractTableService  {
    
    @Override
    public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
        // 将列表转换为IN子句的形式
        String schemasStr = schemas.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        String tablesStr = tables.stream().map(t -> "'" + t + "'").collect(Collectors.joining(","));

        String sql = "SELECT TABLE_NAME, TABLE_SCHEMA AS SCHEMA_NAME " +
            "FROM INFORMATION_SCHEMA.TABLES " +
            "WHERE TABLE_SCHEMA IN (" + schemasStr + ") " +
            "AND TABLE_NAME IN (" + tablesStr + ")";
        log.info("执行SQL: {}", sql);
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            TableEnt tableEnt = new TableEnt();
            tableEnt.setTableName(rs.getString("TABLE_NAME"));
            tableEnt.setSchemaName(rs.getString("SCHEMA_NAME"));
            return tableEnt;
        });
    }
} 
