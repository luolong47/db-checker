package io.github.luolong47.dbchecker.service;

import io.github.luolong47.dbchecker.entity.TableEnt;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service("gaussDBTableService")
public class GaussDBTableService extends AbstractTableService {

    @Override
    public List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables) {
        // 将列表转换为IN子句的形式
        String schemasStr = schemas.stream().map(s -> "'" + s + "'").collect(Collectors.joining(","));
        String tablesStr = tables.stream().map(t -> "'" + t + "'").collect(Collectors.joining(","));

        String sql = "SELECT c.relname AS TABLE_NAME, n.nspname AS SCHEMA_NAME " +
                     "FROM pg_class c " +
                     "JOIN pg_namespace n ON n.oid = c.relnamespace " +
                     "WHERE c.relkind = 'r' " +
                     "AND n.nspname IN (" + schemasStr + ") " +
                     "AND c.relname IN (" + tablesStr + ")";
        
        log.info("执行SQL: {}", sql);
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            TableEnt tableEnt = new TableEnt();
            tableEnt.setTableName(rs.getString("TABLE_NAME"));
            tableEnt.setSchemaName(rs.getString("SCHEMA_NAME"));
            return tableEnt;
        });
    }
} 
