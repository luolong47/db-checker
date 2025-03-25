package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 公式2策略：ora = rlcms_base
 */
public class Formula2Strategy extends AbstractFormulaStrategy {
    public Formula2Strategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    public String getDesc() {
        return "公式2(仅基础): ora = rlcms_base";
    }

    @Override
    protected boolean compareMoneyValues(Map<String, BigDecimal> values) {
        return valueCollector.isApproximatelyEqual(values.get("ora"), values.get("rlcms_base"));
    }

    @Override
    protected boolean compareCountValues(Map<String, Long> values) {
        return values.get("ora").equals(values.get("rlcms_base"));
    }

    @Override
    protected DiffInfo getMoneyDiffInfo(Map<String, BigDecimal> values) {
        BigDecimal diff = values.get("ora").subtract(values.get("rlcms_base"));
        return new DiffInfo(
            String.format("ORA(%s) - RLCMS_BASE(%s)",
                values.get("ora"), values.get("rlcms_base")),
            diff.toString()
        );
    }

    @Override
    protected DiffInfo getCountDiffInfo(Map<String, Long> values) {
        long diff = values.get("ora") - values.get("rlcms_base");
        return new DiffInfo(
            String.format("ORA(%d) - RLCMS_BASE(%d)",
                values.get("ora"), values.get("rlcms_base")),
            String.valueOf(diff)
        );
    }

}