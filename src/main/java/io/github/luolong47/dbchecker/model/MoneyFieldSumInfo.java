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
     * 各数据源中表的记录数量
     * key: COUNT_ORA, COUNT_RLCMS_BASE, COUNT_RLCMS_PV1 etc.
     * value: 对应的记录数
     */
    private Map<String, Long> countValues = new HashMap<>();
    
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
     * key: SUM_ORA, SUM_RLCMS_BASE, SUM_RLCMS_PV1 etc.
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
     * 设置指定数据源的记录数量（基于索引）
     * 
     * @param sourceIndex 数据源索引（1开始）
     * @param value 记录数量
     */
    public void setCountValue(int sourceIndex, Long value) {
        String key;
        switch (sourceIndex) {
            case 1:
                key = "COUNT_ORA";
                break;
            case 2:
                key = "COUNT_RLCMS_BASE";
                break;
            case 3:
                key = "COUNT_RLCMS_PV1";
                break;
            case 4:
                key = "COUNT_RLCMS_PV2";
                break;
            case 5:
                key = "COUNT_RLCMS_PV3";
                break;
            case 6:
                key = "COUNT_BSCOPY_PV1";
                break;
            case 7:
                key = "COUNT_BSCOPY_PV2";
                break;
            case 8:
                key = "COUNT_BSCOPY_PV3";
                break;
            default:
                key = "COUNT" + sourceIndex;
        }
        countValues.put(key, value);
    }
    
    /**
     * 获取指定数据源的记录数量（基于索引）
     * 
     * @param sourceIndex 数据源索引（1开始）
     * @return 记录数量
     */
    public Long getCountValue(int sourceIndex) {
        String key;
        switch (sourceIndex) {
            case 1:
                key = "COUNT_ORA";
                break;
            case 2:
                key = "COUNT_RLCMS_BASE";
                break;
            case 3:
                key = "COUNT_RLCMS_PV1";
                break;
            case 4:
                key = "COUNT_RLCMS_PV2";
                break;
            case 5:
                key = "COUNT_RLCMS_PV3";
                break;
            case 6:
                key = "COUNT_BSCOPY_PV1";
                break;
            case 7:
                key = "COUNT_BSCOPY_PV2";
                break;
            case 8:
                key = "COUNT_BSCOPY_PV3";
                break;
            default:
                key = "COUNT" + sourceIndex;
        }
        return countValues.getOrDefault(key, null);
    }
    
    /**
     * 设置指定数据源的记录数量（基于数据源名称）
     * 
     * @param sourceName 数据源名称
     * @param value 记录数量
     */
    public void setCountValueByName(String sourceName, Long value) {
        countValues.put("COUNT_" + sourceName.toUpperCase().replace('-', '_'), value);
    }
    
    /**
     * 获取指定数据源的记录数量（基于数据源名称）
     * 
     * @param sourceName 数据源名称
     * @return 记录数量
     */
    public Long getCountValueByName(String sourceName) {
        return countValues.getOrDefault("COUNT_" + sourceName.toUpperCase().replace('-', '_'), null);
    }
    
    /**
     * 设置指定数据源的SUM值（基于索引）
     * 
     * @param sourceIndex 数据源索引（1开始）
     * @param value SUM值
     */
    public void setSumValue(int sourceIndex, BigDecimal value) {
        String key;
        switch (sourceIndex) {
            case 1:
                key = "SUM_ORA";
                break;
            case 2:
                key = "SUM_RLCMS_BASE";
                break;
            case 3:
                key = "SUM_RLCMS_PV1";
                break;
            case 4:
                key = "SUM_RLCMS_PV2";
                break;
            case 5:
                key = "SUM_RLCMS_PV3";
                break;
            case 6:
                key = "SUM_BSCOPY_PV1";
                break;
            case 7:
                key = "SUM_BSCOPY_PV2";
                break;
            case 8:
                key = "SUM_BSCOPY_PV3";
                break;
            default:
                key = "SUM" + sourceIndex;
        }
        sumValues.put(key, value);
    }
    
    /**
     * 获取指定数据源的SUM值（基于索引）
     * 
     * @param sourceIndex 数据源索引（1开始）
     * @return SUM值
     */
    public BigDecimal getSumValue(int sourceIndex) {
        String key;
        switch (sourceIndex) {
            case 1:
                key = "SUM_ORA";
                break;
            case 2:
                key = "SUM_RLCMS_BASE";
                break;
            case 3:
                key = "SUM_RLCMS_PV1";
                break;
            case 4:
                key = "SUM_RLCMS_PV2";
                break;
            case 5:
                key = "SUM_RLCMS_PV3";
                break;
            case 6:
                key = "SUM_BSCOPY_PV1";
                break;
            case 7:
                key = "SUM_BSCOPY_PV2";
                break;
            case 8:
                key = "SUM_BSCOPY_PV3";
                break;
            default:
                key = "SUM" + sourceIndex;
        }
        return sumValues.getOrDefault(key, null);
    }
    
    /**
     * 设置指定数据源的SUM值（基于数据源名称）
     * 
     * @param sourceName 数据源名称
     * @param value SUM值
     */
    public void setSumValueByName(String sourceName, BigDecimal value) {
        sumValues.put("SUM_" + sourceName.toUpperCase().replace('-', '_'), value);
    }
    
    /**
     * 获取指定数据源的SUM值（基于数据源名称）
     * 
     * @param sourceName 数据源名称
     * @return SUM值
     */
    public BigDecimal getSumValueByName(String sourceName) {
        return sumValues.getOrDefault("SUM_" + sourceName.toUpperCase().replace('-', '_'), null);
    }
} 