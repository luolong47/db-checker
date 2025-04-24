package io.github.luolong47.dbchecker.entity;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula3 extends AbstractFormula {

    @Override
    public String getDesc() {
        return "公式3: ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3";
    }

    @Override
    protected boolean compareValues(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        BigDecimal bscopyPv1Value = getValueOrZero(colResult, "bscopy-pv1");
        BigDecimal bscopyPv2Value = getValueOrZero(colResult, "bscopy-pv2");
        BigDecimal bscopyPv3Value = getValueOrZero(colResult, "bscopy-pv3");
        
        // 比较值是否都相等
        return oraValue.compareTo(rlcmsBaseValue) == 0
                && oraValue.compareTo(bscopyPv1Value) == 0
                && oraValue.compareTo(bscopyPv2Value) == 0
                && oraValue.compareTo(bscopyPv3Value) == 0;
    }

    @Override
    protected BigDecimal calculateDiff(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        BigDecimal bscopyPv1Value = getValueOrZero(colResult, "bscopy-pv1");
        BigDecimal bscopyPv2Value = getValueOrZero(colResult, "bscopy-pv2");
        BigDecimal bscopyPv3Value = getValueOrZero(colResult, "bscopy-pv3");
        
        // 使用通用方法计算最大差异值
        return calculateMaxDifference(oraValue, rlcmsBaseValue, bscopyPv1Value, bscopyPv2Value, bscopyPv3Value);
    }

    @Override
    protected String getFailureMessage(Map<String, BigDecimal> colResult, BigDecimal diff) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        BigDecimal bscopyPv1Value = getValueOrZero(colResult, "bscopy-pv1");
        BigDecimal bscopyPv2Value = getValueOrZero(colResult, "bscopy-pv2");
        BigDecimal bscopyPv3Value = getValueOrZero(colResult, "bscopy-pv3");
        
        return String.format("公式3验证失败：ora(%s) = rlcms_base(%s) = bscopy_pv1(%s) = bscopy_pv2(%s) = bscopy_pv3(%s)，最大差异值: %s",
                oraValue, rlcmsBaseValue, bscopyPv1Value, bscopyPv2Value, bscopyPv3Value, diff);
    }
} 
