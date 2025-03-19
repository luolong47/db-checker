package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;

/**
 * 定义统一的公式策略接口
 */
public interface FormulaStrategy {
    /**
     * 获取公式描述
     */
    String getDesc();

    /**
     * 计算金额字段求和结果
     */
    String calculateSum(MoneyFieldSumInfo info);

    /**
     * 计算记录数结果
     */
    String calculateCount(MoneyFieldSumInfo info);

    /**
     * 获取金额字段求和差异信息
     */
    DiffInfo getDiffInfoForSum(MoneyFieldSumInfo info);

    /**
     * 获取记录数差异信息
     */
    DiffInfo getDiffInfoForCount(MoneyFieldSumInfo info);
} 