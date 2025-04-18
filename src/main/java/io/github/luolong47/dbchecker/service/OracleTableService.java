package io.github.luolong47.dbchecker.service;

import cn.hutool.core.collection.ListUtil;
import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
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
} 
