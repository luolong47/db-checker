package io.github.luolong47.dbchecker.service;

import org.springframework.jdbc.core.JdbcTemplate;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public abstract class AbstractTableService implements TableService {

    @Override
    public Map<String, List<String>> getDecimalColumnsForTables(JdbcTemplate jdbcTemplate, String schema, List<String> tables, int minDecimalDigits) {
        log.warn("getDecimalColumnsForTables未实现");
        return new ConcurrentHashMap<>();
    }
}
