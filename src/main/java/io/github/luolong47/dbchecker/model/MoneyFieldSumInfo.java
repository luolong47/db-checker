package io.github.luolong47.dbchecker.model;

import cn.hutool.core.annotation.Alias;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * 金额字段SUM结果信息
 */
@Data
public class MoneyFieldSumInfo {
    
    /**
     * 表名
     */
    @Alias("表名")
    private String tableName;
    
    /**
     * 模式/架构
     */
    @Alias("SCHEMA")
    private String schema;
    
    /**
     * 所在数据库
     */
    @Alias("所在库")
    private String dataSources;
    
    /**
     * 表记录数量
     */
    @Alias("COUNT")
    private String recordCounts;
    
    /**
     * 金额字段
     */
    @Alias("金额字段")
    private String moneyFields;
    
    /**
     * SUM字段
     */
    @Alias("SUM字段")
    private String sumField;
    
    /**
     * 各数据源中金额字段的SUM值
     * key: SUM1, SUM2, SUM3 etc.
     * value: 对应的SUM结果
     */
    private Map<String, BigDecimal> sumValues = new HashMap<>();
    
    public MoneyFieldSumInfo() {
    }
    
    public MoneyFieldSumInfo(String tableName, String schema, String dataSources, 
                            String recordCounts, String moneyFields, String sumField) {
        this.tableName = tableName;
        this.schema = schema;
        this.dataSources = dataSources;
        this.recordCounts = recordCounts;
        this.moneyFields = moneyFields;
        this.sumField = sumField;
    }
    
    /**
     * 设置指定数据源的SUM值
     * 
     * @param sourceIndex 数据源索引（1开始）
     * @param value SUM值
     */
    public void setSumValue(int sourceIndex, BigDecimal value) {
        sumValues.put("SUM" + sourceIndex, value);
    }
    
    /**
     * 获取指定数据源的SUM值
     * 
     * @param sourceIndex 数据源索引（1开始）
     * @return SUM值
     */
    public BigDecimal getSumValue(int sourceIndex) {
        return sumValues.getOrDefault("SUM" + sourceIndex, null);
    }
} 