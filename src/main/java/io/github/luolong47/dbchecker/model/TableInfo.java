package io.github.luolong47.dbchecker.model;

import lombok.Data;

import java.math.BigDecimal;
import java.util.*;

/**
 * 表信息类 - 存储表的元数据信息和各种统计结果
 */
@Data
public class TableInfo {
    // 基本属性
    private final String tableName;
    // 记录数
    private final Map<String, Long> recordCounts = new HashMap<>();
    // 金额字段相关
    private final Set<String> moneyFields = new HashSet<>();
    private final Map<String, Map<String, BigDecimal>> moneySums = new HashMap<>();
    // 数据源列表
    private final List<String> dataSources = new ArrayList<>();
    private String dataSourceName;

    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    public void addDataSource(String dataSource) {
        if (!this.dataSources.contains(dataSource)) {
            this.dataSources.add(dataSource);
        }
    }

    public void setRecordCount(String dataSource, long count) {
        this.recordCounts.put(dataSource, count);
    }

    public long getRecordCount() {
        return Optional.ofNullable(dataSourceName)
            .map(ds -> recordCounts.getOrDefault(ds, 0L))
            .orElse(0L);
    }

    public void addMoneyField(String fieldName) {
        this.moneyFields.add(fieldName);
    }

    public void setMoneySum(String dataSource, String fieldName, BigDecimal sum) {
        this.moneySums.computeIfAbsent(dataSource, k -> new HashMap<>()).put(fieldName, sum);
    }

    /**
     * 获取当前数据源所有字段的SUM值
     */
    public Map<String, BigDecimal> getMoneySums() {
        return Optional.ofNullable(dataSourceName)
            .map(ds -> moneySums.getOrDefault(ds, Collections.emptyMap()))
            .orElse(Collections.emptyMap());
    }

    /**
     * 获取所有数据源的所有字段SUM值
     */
    public Map<String, Map<String, BigDecimal>> getAllMoneySums() {
        return moneySums;
    }
} 