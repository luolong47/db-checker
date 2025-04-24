package io.github.luolong47.dbchecker.entity;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula1 extends AbstractFormula {

    @Override
    public String getDesc() {
        return StrUtil.format("公式1: ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3");
    }

    @Override
    protected boolean compareValues(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal sum = calculateRlcmsSum(colResult);
        return oraValue.compareTo(sum) == 0;
    }

    @Override
    protected BigDecimal calculateDiff(Map<String, BigDecimal> colResult) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal sum = calculateRlcmsSum(colResult);
        return oraValue.subtract(sum);
    }
    
    private BigDecimal calculateRlcmsSum(Map<String, BigDecimal> colResult) {
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        BigDecimal rlcmsPv2Value = getValueOrZero(colResult, "rlcms-pv2");
        BigDecimal rlcmsPv3Value = getValueOrZero(colResult, "rlcms-pv3");
        return rlcmsPv1Value.add(rlcmsPv2Value).add(rlcmsPv3Value);
    }

    @Override
    protected String getFailureMessage(Map<String, BigDecimal> colResult, BigDecimal diff) {
        BigDecimal oraValue = getValueOrZero(colResult, "ora");
        BigDecimal rlcmsPv1Value = getValueOrZero(colResult, "rlcms-pv1");
        BigDecimal rlcmsPv2Value = getValueOrZero(colResult, "rlcms-pv2");
        BigDecimal rlcmsPv3Value = getValueOrZero(colResult, "rlcms-pv3");
        
        return StrUtil.format("公式1验证失败：ora({}) != rlcms_pv1({}) + rlcms_pv2({}) + rlcms_pv3({})，差异值: {}",
                oraValue, rlcmsPv1Value, rlcmsPv2Value, rlcmsPv3Value, diff);
    }
}
