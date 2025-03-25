package io.github.luolong47.dbchecker.service.strategy;

/**
 * 公式3策略：ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3
 */
public class Formula3Strategy extends AbstractEqualityFormulaStrategy {
    public Formula3Strategy(ValueCollector valueCollector) {
        super(valueCollector);
    }

    @Override
    public String getDesc() {
        return "公式3(基础+副本): ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3";
    }
} 