package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 公式5策略：ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3
 */
@RequiredArgsConstructor
public class Formula5Strategy implements FormulaStrategy {
    private final String desc = "公式5(全部): ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3";
    private final ValueCollector valueCollector;

    @Override
    public String getDesc() {
        return desc;
    }

    @Override
    public String calculateSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) return "N/A";

        return valueCollector.areAllValuesEqual(values.values().toArray(new BigDecimal[0])) ? "TRUE" : "FALSE";
    }

    @Override
    public String calculateCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) return "N/A";

        return valueCollector.areAllCountValuesEqual(values.values()) ? "TRUE" : "FALSE";
    }

    @Override
    public DiffInfo getDiffInfoForSum(MoneyFieldSumInfo info) {
        Map<String, BigDecimal> values = valueCollector.collectSumValues(info);
        if (containsNull(values)) return new DiffInfo("N/A", "");

        List<String> diffs = new ArrayList<>();
        BigDecimal reference = values.get("ora");
        BigDecimal totalDiff = BigDecimal.ZERO;
        for (Map.Entry<String, BigDecimal> entry : values.entrySet()) {
            if (!entry.getKey().equals("ora")) {
                BigDecimal diff = reference.subtract(entry.getValue());
                totalDiff = totalDiff.add(diff.abs());
                if (diff.abs().compareTo(new BigDecimal("0.01")) > 0) {
                    diffs.add(String.format("%s(%s)", entry.getKey(), entry.getValue()));
                }
            }
        }
        return diffs.isEmpty() ? new DiffInfo("", "") :
            new DiffInfo(
                String.format("ORA(%s)与以下值不相等: %s", reference, String.join("; ", diffs)),
                totalDiff.toString()
            );
    }

    @Override
    public DiffInfo getDiffInfoForCount(MoneyFieldSumInfo info) {
        Map<String, Long> values = valueCollector.collectCountValues(info);
        if (containsNull(values)) return new DiffInfo("N/A", "");

        List<String> diffs = new ArrayList<>();
        Long reference = values.get("ora");
        long totalDiff = 0;
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            if (!entry.getKey().equals("ora") && !reference.equals(entry.getValue())) {
                long diff = Math.abs(reference - entry.getValue());
                totalDiff += diff;
                diffs.add(String.format("%s(%d)", entry.getKey(), entry.getValue()));
            }
        }
        return diffs.isEmpty() ? new DiffInfo("", "") :
            new DiffInfo(
                String.format("ORA(%d)与以下值不相等: %s", reference, String.join("; ", diffs)),
                String.valueOf(totalDiff)
            );
    }

    /**
     * 检查Map中是否包含null值
     */
    private <T> boolean containsNull(Map<String, T> map) {
        return map.values().stream().anyMatch(Objects::isNull);
    }
} 