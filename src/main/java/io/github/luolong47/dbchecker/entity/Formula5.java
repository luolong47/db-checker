package io.github.luolong47.dbchecker.entity;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula5 extends AbstractFormula {

    @Override
    public String getDesc() {
        return "公式5: ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3";
    }

    @Override
    protected boolean compareValues(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        BigDecimal rlcmsPv2Value = getValueOrZero(colResult, "rlcms-pv2");
        BigDecimal rlcmsPv3Value = getValueOrZero(colResult, "rlcms-pv3");
        
        // 比较值是否都相等
        return oraValue.compareTo(rlcmsBaseValue) == 0
                && oraValue.compareTo(rlcmsPv1Value) == 0
                && oraValue.compareTo(rlcmsPv2Value) == 0
                && oraValue.compareTo(rlcmsPv3Value) == 0;
    }

    @Override
    protected BigDecimal calculateDiff(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        BigDecimal rlcmsPv2Value = getValueOrZero(colResult, "rlcms-pv2");
        BigDecimal rlcmsPv3Value = getValueOrZero(colResult, "rlcms-pv3");
        
        // 使用通用方法计算最大差异值
        return calculateMaxDifference(oraValue, rlcmsBaseValue, rlcmsPv1Value, rlcmsPv2Value, rlcmsPv3Value);
    }

    @Override
    protected String getFailureMessage(Map<String, BigDecimal> colResult, BigDecimal diff) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsBaseValue = getValueOrZero(colResult, "rlcms-base");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        BigDecimal rlcmsPv2Value = getValueOrZero(colResult, "rlcms-pv2");
        BigDecimal rlcmsPv3Value = getValueOrZero(colResult, "rlcms-pv3");
        
        return String.format("公式5验证失败：ora(%s) = rlcms_base(%s) = rlcms_pv1(%s) = rlcms_pv2(%s) = rlcms_pv3(%s)，最大差异值: %s",
                oraValue, rlcmsBaseValue, rlcmsPv1Value, rlcmsPv2Value, rlcmsPv3Value, diff);
    }
} 
