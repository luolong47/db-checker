package io.github.luolong47.dbchecker.model;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;

/**
 * 表信息类，用于存储数据库表的元数据信息和统计结果。
 * <p>
 * 该类主要用于：
 * <ul>
 *   <li>存储表的基本信息（表名、数据源等）</li>
 *   <li>记录表的记录数统计（含条件和无条件）</li>
 *   <li>管理表中的金额字段信息</li>
 *   <li>存储金额字段的求和结果（含条件和无条件）</li>
 * </ul>
 * <p>
 * 该类支持多数据源场景，可以同时存储不同数据源的统计结果。
 *
 * @author luolong47
 */
@Data
@Slf4j
public class TableInfo {
    /**
     * 表名
     */
    private final String tableName;

    /** 各数据源的记录数统计（满足条件的记录数），key为数据源名称 */
    private final Map<String, Long> recordCounts = new HashMap<>();

    /** 各数据源的总记录数统计（无条件），key为数据源名称 */
    private final Map<String, Long> recordCountsAll = new HashMap<>();

    /** 表中的金额字段集合 */
    private final Set<String> moneyFields = new HashSet<>();

    /** 金额字段的求和结果（满足条件），第一层key为数据源名称，第二层key为字段名称 */
    private final Map<String, Map<String, BigDecimal>> moneySums = new HashMap<>();

    /** 金额字段的总和结果（无条件），第一层key为数据源名称，第二层key为字段名称 */
    private final Map<String, Map<String, BigDecimal>> moneySumsAll = new HashMap<>();

    /** 关联的数据源列表 */
    private final List<String> dataSources = new ArrayList<>();

    /** 当前使用的数据源名称 */
    private String dataSourceName;

    /**
     * 构造函数
     *
     * @param tableName 表名
     */
    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    /**
     * 添加数据源到数据源列表中
     *
     * @param dataSource 数据源名称
     */
    public void addDataSource(String dataSource) {
        if (!this.dataSources.contains(dataSource)) {
            this.dataSources.add(dataSource);
        }
    }

    /**
     * 设置指定数据源的记录数（满足条件的记录数）
     *
     * @param dataSource 数据源名称
     * @param count 记录数
     */
    public void setRecordCount(String dataSource, long count) {
        this.recordCounts.put(dataSource, count);
    }

    /**
     * 获取当前数据源的记录数（满足条件的记录数）
     *
     * @return 记录数，如果未设置数据源或记录数不存在则返回0
     */
    public long getRecordCount() {
        return Optional.ofNullable(dataSourceName)
            .map(ds -> recordCounts.getOrDefault(ds, 0L))
            .orElse(0L);
    }

    /**
     * 设置指定数据源的总记录数（无条件）
     *
     * @param dataSource 数据源名称
     * @param count 总记录数
     */
    public void setRecordCountAll(String dataSource, long count) {
        this.recordCountsAll.put(dataSource, count);
    }

    /**
     * 获取指定数据源的总记录数（无条件）
     *
     * @param dataSource 数据源名称
     * @return 总记录数，如果数据源不存在则返回0
     */
    public long getRecordCountAll(String dataSource) {
        return recordCountsAll.getOrDefault(dataSource, 0L);
    }

    /**
     * 添加金额字段
     *
     * @param fieldName 字段名称
     */
    public void addMoneyField(String fieldName) {
        this.moneyFields.add(fieldName);
    }

    /**
     * 设置指定数据源和字段的金额求和结果（满足条件）
     *
     * @param dataSource 数据源名称
     * @param fieldName 字段名称
     * @param sum 求和结果
     */
    public void setMoneySum(String dataSource, String fieldName, BigDecimal sum) {
        this.moneySums.computeIfAbsent(dataSource, k -> new HashMap<>()).put(fieldName, sum);
    }

    /**
     * 设置指定数据源和字段的金额总和结果（无条件）
     *
     * @param dataSource 数据源名称
     * @param fieldName 字段名称
     * @param sum 总和结果
     */
    public void setMoneySumAll(String dataSource, String fieldName, BigDecimal sum) {
        this.moneySumsAll.computeIfAbsent(dataSource, k -> new HashMap<>()).put(fieldName, sum);
        log.info("设置表[{}]字段[{}]在数据源[{}]中的无条件SUM值到moneySumsAll: {}", tableName, fieldName, dataSource, sum);
    }

    /**
     * 获取当前数据源所有字段的求和结果（满足条件）
     *
     * @return 字段求和结果映射，key为字段名称，value为求和结果。如果未设置数据源则返回空映射
     */
    public Map<String, BigDecimal> getMoneySums() {
        return Optional.ofNullable(dataSourceName)
            .map(ds -> moneySums.getOrDefault(ds, Collections.emptyMap()))
            .orElse(Collections.emptyMap());
    }

    /**
     * 获取所有数据源的所有字段求和结果（满足条件）
     *
     * @return 数据源和字段求和结果的嵌套映射，第一层key为数据源名称，第二层key为字段名称
     */
    public Map<String, Map<String, BigDecimal>> getAllMoneySums() {
        return moneySums;
    }

    /**
     * 获取所有数据源的所有字段总和结果（无条件）
     *
     * @return 数据源和字段总和结果的嵌套映射，第一层key为数据源名称，第二层key为字段名称
     */
    public Map<String, Map<String, BigDecimal>> getAllMoneySumsAll() {
        log.info("获取表[{}]的所有无条件SUM值: {}", tableName, moneySumsAll);
        return moneySumsAll;
    }
}