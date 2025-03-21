package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * 公式1策略：ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3
 */
@RequiredArgsConstructor
public class Formula1Strategy implements FormulaStrategy {
    private final String desc = "公式1(分省-拆分): ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3";
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

        BigDecimal sum = values.get("rlcms_pv1")
            .add(values.get("rlcms_pv2"))
            .add(values.get("rlcms_pv3"));
        return valueCollector.isApproximatelyEqual(values.get("ora"), sum) ? "TRUE" : "FALSE";
    }

    @Override
    public String calculateCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return "N/A";
        }

        long sum = values.get("rlcms_pv1") + values.get("rlcms_pv2") + values.get("rlcms_pv3");
        return values.get("ora").equals(sum) ? "TRUE" : "FALSE";
    }

    @Override
    public DiffInfo getDiffInfoForSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }

        BigDecimal sum = values.get("rlcms_pv1")
            .add(values.get("rlcms_pv2"))
            .add(values.get("rlcms_pv3"));
        BigDecimal diff = values.get("ora").subtract(sum);
        return new DiffInfo(
            String.format("ORA(%s) - (RLCMS_PV1(%s) + RLCMS_PV2(%s) + RLCMS_PV3(%s))",
                values.get("ora"), values.get("rlcms_pv1"),
                values.get("rlcms_pv2"), values.get("rlcms_pv3")),
            diff.toString()
        );
    }

    @Override
    public DiffInfo getDiffInfoForCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) {
            return new DiffInfo("N/A", "");
        }

        long sum = values.get("rlcms_pv1") + values.get("rlcms_pv2") + values.get("rlcms_pv3");
        long diff = values.get("ora") - sum;
        return new DiffInfo(
            String.format("ORA(%d) - (RLCMS_PV1(%d) + RLCMS_PV2(%d) + RLCMS_PV3(%d))",
                values.get("ora"), values.get("rlcms_pv1"),
                values.get("rlcms_pv2"), values.get("rlcms_pv3")),
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