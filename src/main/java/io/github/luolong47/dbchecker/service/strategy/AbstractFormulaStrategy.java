package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * 公式策略的抽象基类，提供通用实现
 */
@RequiredArgsConstructor
public abstract class AbstractFormulaStrategy implements FormulaStrategy {
    protected final ValueCollector valueCollector;

    /**
     * 比较金额是否符合策略要求
     *
     * @param values 各数据源的金额值
     * @return 是否符合要求
     */
    protected abstract boolean compareMoneyValues(Map<String, BigDecimal> values);

    /**
     * 比较记录数是否符合策略要求
     *
     * @param values 各数据源的记录数
     * @return 是否符合要求
     */
    protected abstract boolean compareCountValues(Map<String, Long> values);

    /**
     * 获取金额差异信息
     *
     * @param values 各数据源的金额值
     * @return 差异信息
     */
    protected abstract DiffInfo getMoneyDiffInfo(Map<String, BigDecimal> values);

    /**
     * 获取记录数差异信息
     *
     * @param values 各数据源的记录数
     * @return 差异信息
     */
    protected abstract DiffInfo getCountDiffInfo(Map<String, Long> values);

    @Override
    public String calculateSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) {
            return "N/A";
        }
        return compareMoneyValues(values) ? "TRUE" : "FALSE";
    }

    @Override
    public String calculateCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return "N/A";
        }
        return compareCountValues(values) ? "TRUE" : "FALSE";
    }

    @Override
    public DiffInfo getDiffInfoForSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }
        return getMoneyDiffInfo(values);
    }

    @Override
    public DiffInfo getDiffInfoForCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }
        return getCountDiffInfo(values);
    }

    /**
     * 检查Map中是否包含null值
     */
    protected <T> boolean containsNull(Map<String, T> map) {
        return map.values().stream().anyMatch(Objects::isNull);
    }
} 