package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;

/**
 * 值收集器接口
 */
public interface ValueCollector {
    /**
     * 收集求和值
     */
    Map<String, BigDecimal> collectSumValues(MoneyFieldSumInfo info);

    /**
     * 收集计数值
     */
    Map<String, Long> collectCountValues(MoneyFieldSumInfo info);

    /**
     * 检查两个BigDecimal是否近似相等（差值小于0.01）
     */
    boolean isApproximatelyEqual(BigDecimal a, BigDecimal b);

    /**
     * 检查所有BigDecimal值是否近似相等
     */
    boolean areAllValuesEqual(BigDecimal... values);

    /**
     * 检查所有COUNT值是否相等
     */
    boolean areAllCountValuesEqual(Collection<Long> values);
} 