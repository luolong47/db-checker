package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 处理相等比较的公式策略抽象基类
 */
public abstract class AbstractEqualityFormulaStrategy extends AbstractFormulaStrategy {
    public AbstractEqualityFormulaStrategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    protected boolean compareMoneyValues(Map<String, BigDecimal> values) {
        return valueCollector.areAllValuesEqual(values.values().toArray(new BigDecimal[0]));
    }

    @Override
    protected boolean compareCountValues(Map<String, Long> values) {
        return valueCollector.areAllCountValuesEqual(values.values());
    }

    @Override
    protected DiffInfo getMoneyDiffInfo(Map<String, BigDecimal> values) {
        List<String> diffs = new ArrayList<>();
        BigDecimal reference = values.get("ora");
        BigDecimal totalDiff = BigDecimal.ZERO;

        for (Map.Entry<String, BigDecimal> entry : values.entrySet()) {
            if (!"ora".equals(entry.getKey())) {
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
    protected DiffInfo getCountDiffInfo(Map<String, Long> values) {
        List<String> diffs = new ArrayList<>();
        Long reference = values.get("ora");
        long totalDiff = 0;

        for (Map.Entry<String, Long> entry : values.entrySet()) {
            if (!"ora".equals(entry.getKey()) && !reference.equals(entry.getValue())) {
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
} 