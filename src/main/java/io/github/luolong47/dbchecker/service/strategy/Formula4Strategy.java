package io.github.luolong47.dbchecker.service.strategy;

/**
 * 公式4策略：ora = rlcms_pv1 = rlcms_pv2 = rlcms_pv3
 */
public class Formula4Strategy extends AbstractEqualityFormulaStrategy {
    public Formula4Strategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    public String getDesc() {
        return "公式4(分省-冗余): ora = rlcms_pv1 = rlcms_pv2 = rlcms_pv3";
    }
} 