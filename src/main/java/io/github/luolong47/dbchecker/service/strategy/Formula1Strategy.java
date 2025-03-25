package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.model.DiffInfo;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 公式1策略：ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3
 */
public class Formula1Strategy extends AbstractFormulaStrategy {
    public Formula1Strategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    public String getDesc() {
        return "公式1(分省-拆分): ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3";
    }

    @Override
    protected boolean compareMoneyValues(Map<String, BigDecimal> values) {
        BigDecimal sum = values.get("rlcms_pv1")
            .add(values.get("rlcms_pv2"))
            .add(values.get("rlcms_pv3"));
        return valueCollector.isApproximatelyEqual(values.get("ora"), sum);
    }

    @Override
    protected boolean compareCountValues(Map<String, Long> values) {
        long sum = values.get("rlcms_pv1") + values.get("rlcms_pv2") + values.get("rlcms_pv3");
        return values.get("ora").equals(sum);
    }

    @Override
    protected DiffInfo getMoneyDiffInfo(Map<String, BigDecimal> values) {
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
    protected DiffInfo getCountDiffInfo(Map<String, Long> values) {
        long sum = values.get("rlcms_pv1") + values.get("rlcms_pv2") + values.get("rlcms_pv3");
        long diff = values.get("ora") - sum;
        return new DiffInfo(
            String.format("ORA(%d) - (RLCMS_PV1(%d) + RLCMS_PV2(%d) + RLCMS_PV3(%d))",
                values.get("ora"), values.get("rlcms_pv1"),
                values.get("rlcms_pv2"), values.get("rlcms_pv3")),
            String.valueOf(diff)
        );
    }

}