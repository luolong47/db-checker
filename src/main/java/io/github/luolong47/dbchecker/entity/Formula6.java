package io.github.luolong47.dbchecker.entity;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula6 extends AbstractFormula {

    @Override
    public String getDesc() {
        return "公式6: ora = rlcms_pv1";
    }

    @Override
    protected boolean compareValues(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        return oraValue.compareTo(rlcmsPv1Value) == 0;
    }

    @Override
    protected BigDecimal calculateDiff(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        return oraValue.subtract(rlcmsPv1Value);
    }

    @Override
    protected String getFailureMessage(Map<String, BigDecimal> colResult, BigDecimal diff) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        
        return String.format("公式6验证失败：ora(%s) != rlcms_pv1(%s)，差异值: %s",
                oraValue, rlcmsPv1Value, diff);
    }
} 
