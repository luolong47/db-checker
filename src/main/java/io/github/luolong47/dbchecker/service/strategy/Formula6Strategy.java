package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * 公式6策略：ora = rlcms_pv1
 */
@RequiredArgsConstructor
public class Formula6Strategy implements FormulaStrategy {
    private final String desc = "公式6(分省1): ora = rlcms_pv1";
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

        return valueCollector.isApproximatelyEqual(values.get("ora"), values.get("rlcms_pv1")) ? "TRUE" : "FALSE";
    }

    @Override
    public String calculateCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return "N/A";
        }

        return values.get("ora").equals(values.get("rlcms_pv1")) ? "TRUE" : "FALSE";
    }

    @Override
    public DiffInfo getDiffInfoForSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }

        BigDecimal diff = values.get("ora").subtract(values.get("rlcms_pv1"));
        return new DiffInfo(
            String.format("ORA(%s) - RLCMS_PV1(%s)",
                values.get("ora"), values.get("rlcms_pv1")),
            diff.toString()
        );
    }

    @Override
    public DiffInfo getDiffInfoForCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }

        long diff = values.get("ora") - values.get("rlcms_pv1");
        return new DiffInfo(
            String.format("ORA(%d) - RLCMS_PV1(%d)",
                values.get("ora"), values.get("rlcms_pv1")),
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