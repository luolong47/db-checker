package io.github.luolong47.dbchecker.entity;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class TableCsvResult {
    private String tableName;
    private String schemaName;
    private List<String> dbs;
    private List<String> sumCols;
    private Map<String,Map<String,Map<String,Long>>> sumMap; //taableName->(db->(sumCols->sumVal))
}
