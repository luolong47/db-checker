package io.github.luolong47.dbchecker.entity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula5 implements Formula {

    @Override
    public String getName() {
        return "公式5";
    }

    @Override
    public String getDesc() {
        return "公式5: ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3";
    }

    @Override
    public boolean result(TableInfo tableInfo,String col) {
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            log.warn("表 [{}] 列 [{}] 没有求和结果，无法验证公式5", tableInfo.getTableName(), col);
            return false;
        }
        
        Map<String, BigDecimal> colResult = sumResult.get(col);
        
        // 获取各库的值
        BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
        BigDecimal rlcmsBaseValue = colResult.getOrDefault("rlcms-base", BigDecimal.ZERO);
        BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
        BigDecimal rlcmsPv2Value = colResult.getOrDefault("rlcms-pv2", BigDecimal.ZERO);
        BigDecimal rlcmsPv3Value = colResult.getOrDefault("rlcms-pv3", BigDecimal.ZERO);
        
        // 比较值是否都相等
        return oraValue.compareTo(rlcmsBaseValue) == 0
                && oraValue.compareTo(rlcmsPv1Value) == 0
                && oraValue.compareTo(rlcmsPv2Value) == 0
                && oraValue.compareTo(rlcmsPv3Value) == 0;
    }

    @Override
    public BigDecimal diff(TableInfo tableInfo,String col) {
        // 计算最大差异值
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            return BigDecimal.ZERO;
        }
        
        Map<String, BigDecimal> colResult = sumResult.get(col);
        
        // 获取各库的值
        BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
        BigDecimal rlcmsBaseValue = colResult.getOrDefault("rlcms-base", BigDecimal.ZERO);
        BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
        BigDecimal rlcmsPv2Value = colResult.getOrDefault("rlcms-pv2", BigDecimal.ZERO);
        BigDecimal rlcmsPv3Value = colResult.getOrDefault("rlcms-pv3", BigDecimal.ZERO);
        
        // 计算与ora的差异值，并找出最大的差异值
        BigDecimal diff1 = oraValue.subtract(rlcmsBaseValue).abs();
        BigDecimal diff2 = oraValue.subtract(rlcmsPv1Value).abs();
        BigDecimal diff3 = oraValue.subtract(rlcmsPv2Value).abs();
        BigDecimal diff4 = oraValue.subtract(rlcmsPv3Value).abs();
        
        BigDecimal maxDiff = diff1;
        if (diff2.compareTo(maxDiff) > 0) maxDiff = diff2;
        if (diff3.compareTo(maxDiff) > 0) maxDiff = diff3;
        if (diff4.compareTo(maxDiff) > 0) maxDiff = diff4;
        
        return maxDiff;
    }

    @Override
    public String diffDesc(TableInfo tableInfo,String col) {
        BigDecimal maxDiff = diff(tableInfo, col);
        if (maxDiff.compareTo(BigDecimal.ZERO) == 0) {
            return "公式5验证通过：ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3";
        } else {
            Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
            Map<String, BigDecimal> colResult = sumResult.get(col);
            
            BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
            BigDecimal rlcmsBaseValue = colResult.getOrDefault("rlcms-base", BigDecimal.ZERO);
            BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
            BigDecimal rlcmsPv2Value = colResult.getOrDefault("rlcms-pv2", BigDecimal.ZERO);
            BigDecimal rlcmsPv3Value = colResult.getOrDefault("rlcms-pv3", BigDecimal.ZERO);
            
            return String.format("公式5验证失败：ora(%s) = rlcms_base(%s) = rlcms_pv1(%s) = rlcms_pv2(%s) = rlcms_pv3(%s)，最大差异值: %s",
                    oraValue, rlcmsBaseValue, rlcmsPv1Value, rlcmsPv2Value, rlcmsPv3Value, maxDiff);
        }
    }
} 
