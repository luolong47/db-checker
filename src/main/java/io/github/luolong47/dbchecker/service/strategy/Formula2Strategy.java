package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * 公式2策略：ora = rlcms_base
 */
@RequiredArgsConstructor
public class Formula2Strategy implements FormulaStrategy {
    private final String desc = "公式2(仅基础): ora = rlcms_base";
    private final ValueCollector valueCollector;

    @Override
    public String getDesc() {
        return desc;
    }

    @Override
    public String calculateSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) {
            return "N/A";
        }

        return valueCollector.isApproximatelyEqual(values.get("ora"), values.get("rlcms_base")) ? "TRUE" : "FALSE";
    }

    @Override
    public String calculateCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return "N/A";
        }

        return values.get("ora").equals(values.get("rlcms_base")) ? "TRUE" : "FALSE";
    }

    @Override
    public DiffInfo getDiffInfoForSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }

        BigDecimal diff = values.get("ora").subtract(values.get("rlcms_base"));
        return new DiffInfo(
            String.format("ORA(%s) - RLCMS_BASE(%s)",
                values.get("ora"), values.get("rlcms_base")),
            diff.toString()
        );
    }

    @Override
    public DiffInfo getDiffInfoForCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }

        long diff = values.get("ora") - values.get("rlcms_base");
        return new DiffInfo(
            String.format("ORA(%d) - RLCMS_BASE(%d)",
                values.get("ora"), values.get("rlcms_base")),
            String.valueOf(diff)
        );
    }

    /**
     * 检查Map中是否包含null值
     */
    private <T> boolean containsNull(Map<String, T> map) {
        return map.values().stream().anyMatch(Objects::isNull);
    }
} 