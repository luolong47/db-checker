package io.github.luolong47.dbchecker.entity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Data
@Slf4j
public class Formula3 implements Formula {
    @Override
    public String getName() {
        return "公式3";
    }

    @Override
    public String getDesc() {
        return "公式3: ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3";
    }

    @Override
    public boolean result(TableInfo tableInfo,String col) {
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            log.warn("表 [{}] 列 [{}] 没有求和结果，无法验证公式3", tableInfo.getTableName(), col);
            return false;
        }
        
        Map<String, BigDecimal> colResult = sumResult.get(col);
        
        // 获取各库的值
        BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
        BigDecimal rlcmsBaseValue = colResult.getOrDefault("rlcms-base", BigDecimal.ZERO);
        BigDecimal bscopyPv1Value = colResult.getOrDefault("bscopy-pv1", BigDecimal.ZERO);
        BigDecimal bscopyPv2Value = colResult.getOrDefault("bscopy-pv2", BigDecimal.ZERO);
        BigDecimal bscopyPv3Value = colResult.getOrDefault("bscopy-pv3", BigDecimal.ZERO);
        
        // 比较值是否都相等
        return oraValue.compareTo(rlcmsBaseValue) == 0
                && oraValue.compareTo(bscopyPv1Value) == 0
                && oraValue.compareTo(bscopyPv2Value) == 0
                && oraValue.compareTo(bscopyPv3Value) == 0;
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
        BigDecimal bscopyPv1Value = colResult.getOrDefault("bscopy-pv1", BigDecimal.ZERO);
        BigDecimal bscopyPv2Value = colResult.getOrDefault("bscopy-pv2", BigDecimal.ZERO);
        BigDecimal bscopyPv3Value = colResult.getOrDefault("bscopy-pv3", BigDecimal.ZERO);
        
        // 计算与ora的差异值，并找出最大的差异值
        BigDecimal diff1 = oraValue.subtract(rlcmsBaseValue).abs();
        BigDecimal diff2 = oraValue.subtract(bscopyPv1Value).abs();
        BigDecimal diff3 = oraValue.subtract(bscopyPv2Value).abs();
        BigDecimal diff4 = oraValue.subtract(bscopyPv3Value).abs();
        
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
            return "公式3验证通过：ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3";
        } else {
            Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
            Map<String, BigDecimal> colResult = sumResult.get(col);
            
            BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
            BigDecimal rlcmsBaseValue = colResult.getOrDefault("rlcms-base", BigDecimal.ZERO);
            BigDecimal bscopyPv1Value = colResult.getOrDefault("bscopy-pv1", BigDecimal.ZERO);
            BigDecimal bscopyPv2Value = colResult.getOrDefault("bscopy-pv2", BigDecimal.ZERO);
            BigDecimal bscopyPv3Value = colResult.getOrDefault("bscopy-pv3", BigDecimal.ZERO);
            
            return String.format("公式3验证失败：ora(%s) = rlcms_base(%s) = bscopy_pv1(%s) = bscopy_pv2(%s) = bscopy_pv3(%s)，最大差异值: %s",
                    oraValue, rlcmsBaseValue, bscopyPv1Value, bscopyPv2Value, bscopyPv3Value, maxDiff);
        }
    }
} 
