package io.github.luolong47.dbchecker.entity;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula2 extends AbstractFormula {

    @Override
    public String getDesc() {
        return "公式2: ora = rlcms_base";
    }

    @Override
    protected boolean compareValues(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        return oraValue.compareTo(rlcmsBaseValue) == 0;
    }

    @Override
    protected BigDecimal calculateDiff(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        return oraValue.subtract(rlcmsBaseValue);
    }

    @Override
    protected String getFailureMessage(Map<String, BigDecimal> colResult, BigDecimal diff) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        
        return String.format("公式2验证失败：ora(%s) != rlcms_base(%s)，差异值: %s",
                oraValue, rlcmsBaseValue, diff);
    }
} 
