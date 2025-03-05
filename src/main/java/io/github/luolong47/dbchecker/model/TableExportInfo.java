package io.github.luolong47.dbchecker.model;

import cn.hutool.core.annotation.Alias;
import lombok.Data;

/**
 * 表导出信息
 */
@Data
public class TableExportInfo {
    
    @Alias("表名")
    private String tableName;
    
    @Alias("SCHEMA")
    private String schema;
    
    @Alias("所在库")
    private String dataSources;
    
    @Alias("COUNT")
    private String recordCounts;
    
    @Alias("金额字段")
    private String moneyFields;
    
    /**
     * 创建导出信息对象
     */
    public static TableExportInfo create(String tableName, String schema, String dataSources, 
                                      String recordCounts, String moneyFields) {
        TableExportInfo info = new TableExportInfo();
        info.setTableName(tableName);
        info.setSchema(schema);
        info.setDataSources(dataSources);
        info.setRecordCounts(recordCounts);
        info.setMoneyFields(moneyFields);
        return info;
    }
}
