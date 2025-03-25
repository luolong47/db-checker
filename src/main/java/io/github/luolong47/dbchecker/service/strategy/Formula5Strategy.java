package io.github.luolong47.dbchecker.service.strategy;

/**
 * 公式5策略：ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3
 */
public class Formula5Strategy extends AbstractEqualityFormulaStrategy {
    public Formula5Strategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    public String getDesc() {
        return "公式5(全部): ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3";
    }
} 