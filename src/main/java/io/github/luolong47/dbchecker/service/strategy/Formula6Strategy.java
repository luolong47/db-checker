package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 公式6策略：ora = rlcms_pv1
 */
public class Formula6Strategy extends AbstractFormulaStrategy {
    public Formula6Strategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    public String getDesc() {
        return "公式6(分省1): ora = rlcms_pv1";
    }

    @Override
    protected boolean compareMoneyValues(Map<String, BigDecimal> values) {
        return valueCollector.isApproximatelyEqual(values.get("ora"), values.get("rlcms_pv1"));
    }

    @Override
    protected boolean compareCountValues(Map<String, Long> values) {
        return values.get("ora").equals(values.get("rlcms_pv1"));
    }

    @Override
    protected DiffInfo getMoneyDiffInfo(Map<String, BigDecimal> values) {
        BigDecimal diff = values.get("ora").subtract(values.get("rlcms_pv1"));
        return new DiffInfo(
            String.format("ORA(%s) - RLCMS_PV1(%s)",
                values.get("ora"), values.get("rlcms_pv1")),
            diff.toString()
        );
    }

    @Override
    protected DiffInfo getCountDiffInfo(Map<String, Long> values) {
        long diff = values.get("ora") - values.get("rlcms_pv1");
        return new DiffInfo(
            String.format("ORA(%d) - RLCMS_PV1(%d)",
                values.get("ora"), values.get("rlcms_pv1")),
            String.valueOf(diff)
        );
    }

}