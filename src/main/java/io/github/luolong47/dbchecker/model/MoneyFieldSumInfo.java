package io.github.luolong47.dbchecker.model;

import cn.hutool.core.annotation.Alias;
import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 金额字段SUM信息类，用于统计和导出
 */
@Data
@Slf4j
public class MoneyFieldSumInfo {
    // 表名
    private final String tableName;
    // 求和字段名称
    private final String sumField;
    // 是否为记录数特殊字段
    private final boolean isCountField;
    // 原始表信息引用
    private final TableInfo tableInfo;

    /**
     * 表所在的schema
     */
    private String schema;

    /**
     * 表所在的数据源列表
     */
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
     * 各数据源中金额字段的SUM值
     * key: SUM_ORA, SUM_RLCMS_BASE, SUM_RLCMS_PV1 etc.
     * value: 对应的SUM结果
     */
    private Map<String, BigDecimal> sumValues = new HashMap<>();

    /**
     * 各数据源中金额字段的无WHERE条件SUM值
     * key: SUM_ORA_ALL, SUM_RLCMS_BASE_ALL, SUM_RLCMS_PV1_ALL etc.
     * value: 对应的无WHERE条件SUM结果
     */
    private Map<String, BigDecimal> sumValuesAll = new HashMap<>();

    /**
     * 根据表元数据和指定的金额字段构造对象
     *
     * @param tableName  表名
     * @param tableInfo  表信息
     * @param moneyField 金额字段
     */
    public MoneyFieldSumInfo(String tableName, TableInfo tableInfo, String moneyField) {
        this.tableName = tableName;
        this.tableInfo = tableInfo;
        this.sumField = moneyField;
        this.isCountField = "_COUNT".equals(moneyField);
        if (tableInfo != null) {
            // TableInfo没有schema字段，暂时留空
            this.schema = "";

            // 将数据源列表转换为字符串
            this.dataSources = String.join(", ", tableInfo.getDataSources());
            this.recordCounts = String.valueOf(tableInfo.getRecordCount());

            // 初始化COUNT值
            for (String ds : tableInfo.getDataSources()) {
                // 设置记录数
                Long count = tableInfo.getRecordCounts().get(ds);
                if (count != null) {
                    this.setCountValueByName(ds, count);
                }
            }

            // 如果是金额字段，则初始化SUM值
            if (!"".equals(moneyField)) {
                for (String ds : tableInfo.getDataSources()) {
                    // 获取该数据源下该字段的SUM值
                    Map<String, BigDecimal> fieldSums = tableInfo.getAllMoneySums().get(ds);
                    if (fieldSums != null && fieldSums.containsKey(moneyField)) {
                        BigDecimal sum = fieldSums.get(moneyField);
                        this.setSumValueByName(ds, sum);
                    }

                    // 获取该数据源下该字段的无WHERE条件SUM值
                    Map<String, BigDecimal> fieldSumsAll = tableInfo.getAllMoneySumsAll().get(ds);
                    if (fieldSumsAll != null && fieldSumsAll.containsKey(moneyField)) {
                        BigDecimal sumAll = fieldSumsAll.get(moneyField);
                        this.setSumValueAllByName(ds, sumAll);
                    }
                }
            }
        }
        // 设置金额字段名称，如果是记录数统计字段，则设置为"记录数统计"
        this.moneyFields = isCountField ? "记录数统计" : moneyField;
    }

    /**
     * 设置指定数据源的记录数量（基于数据源名称）
     *
     * @param sourceName 数据源名称
     * @param value      记录数量
     */
    public void setCountValueByName(String sourceName, Long value) {
        countValues.put("COUNT_" + sourceName.toUpperCase().replace('-', '_'), value);
    }

    /**
     * 设置指定数据源的SUM值（基于数据源名称）
     *
     * @param sourceName 数据源名称
     * @param value      SUM值
     */
    public void setSumValueByName(String sourceName, BigDecimal value) {
        sumValues.put("SUM_" + sourceName.toUpperCase().replace('-', '_'), value);
    }

    /**
     * 设置指定数据源的无WHERE条件SUM值
     *
     * @param sourceName 数据源名称
     * @param value      无WHERE条件SUM值
     */
    public void setSumValueAllByName(String sourceName, BigDecimal value) {
        sumValuesAll.put("SUM_" + sourceName.toUpperCase().replace('-', '_') + "_ALL", value);
    }

    /**
     * 获取表所在的数据源列表，以逗号分隔
     */
    public String getDataSources() {
        return StrUtil.join(", ", tableInfo.getDataSources());
    }

    /**
     * 获取表的所有金额字段，以逗号分隔
     */
    public String getMoneyFields() {
        return isCountField ? "记录数统计" : sumField;
    }

    /**
     * 获取统计项名称
     */
    public String getSumField() {
        return isCountField ? "_COUNT" : sumField;
    }

    /**
     * 获取指定数据源的SUM值
     */
    public BigDecimal getSumValueByDataSource(String dataSource) {
        if (isCountField) {
            // COUNT字段不返回SUM值
            return null;
        }

        // 只有当表所在数据源列表中包含该数据源时，才返回对应的SUM值
        if (tableInfo != null && tableInfo.getDataSources().contains(dataSource)) {
            return Optional.ofNullable(tableInfo.getAllMoneySums().get(dataSource))
                .map(fieldMap -> fieldMap.get(sumField))
                .orElse(null);
        }

        // 对于不在表数据源列表中的数据源，返回null
        return null;
    }

    /**
     * 获取指定数据源的记录数
     */
    public Long getCountValueByDataSource(String dataSource) {
        if (!isCountField) {
            // 非COUNT字段不返回记录数
            return null;
        }

        // 只有当表所在数据源列表中包含该数据源时，才返回对应的记录数
        if (tableInfo != null && tableInfo.getDataSources().contains(dataSource)) {
            return tableInfo.getRecordCounts().getOrDefault(dataSource, 0L);
        }

        // 对于不在表数据源列表中的数据源，返回null
        return null;
    }

    /**
     * 获取指定数据源的无WHERE条件SUM值
     */
    public BigDecimal getSumValueAllByDataSource(String dataSource) {
        if (isCountField) {
            // 对于记录数字段，返回无条件记录数
            if (tableInfo != null && tableInfo.getDataSources().contains(dataSource)) {
                long countAll = tableInfo.getRecordCountAll(dataSource);
                log.debug("获取表[{}]在数据源[{}]中的无条件记录数: {}", tableName, dataSource, countAll);
                return BigDecimal.valueOf(countAll);
            }
            return null;
        }

        // 输出实际存储的所有键值对
        log.debug("表[{}]字段[{}]的moneySumsAll内容: {}", tableName, sumField, tableInfo.getAllMoneySumsAll());

        // 只有当表所在数据源列表中包含该数据源时，才返回对应的无WHERE条件SUM值
        if (tableInfo.getDataSources().contains(dataSource)) {
            Map<String, BigDecimal> fieldMap = tableInfo.getAllMoneySumsAll().get(dataSource);
            log.debug("获取表[{}]字段[{}]在数据源[{}]中的无条件SUM值, fieldMap: {}, sumField: {}",
                tableName, sumField, dataSource, fieldMap, sumField);

            BigDecimal result = Optional.ofNullable(fieldMap)
                .map(map -> map.get(sumField))
                .orElse(null);

            log.debug("表[{}]字段[{}]在数据源[{}]中的无条件SUM值结果: {}",
                tableName, sumField, dataSource, result);

            return result;
        }

        // 对于不在表数据源列表中的数据源，返回null
        return null;
    }
}