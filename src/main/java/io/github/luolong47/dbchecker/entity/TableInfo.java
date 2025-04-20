package io.github.luolong47.dbchecker.entity;

import lombok.Data;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Data
public class TableInfo {
    private String tableName;
    private String schemaName;
    private List<String> dbs;
    private List<String> sumCols;
    private Map<String, Map<String, BigDecimal>> sumResult; //sum->(db->value)
    private Formula formula;

    public TableInfo(String tableName, List<String> dbs) {
        this.tableName = tableName;
        this.dbs = dbs;
    }
}
