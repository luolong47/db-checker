package io.github.luolong47.dbchecker.entity;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Data
@Slf4j
public class Formula6 implements Formula {

    @Override
    public String getDesc() {
        return "公式6: ora = rlcms_pv1";
    }

    @Override
    public boolean result(TableInfo tableInfo,String col) {
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            log.warn("表 [{}] 列 [{}] 没有求和结果，无法验证公式6", tableInfo.getTableName(), col);
            return false;
        }
        
        Map<String, BigDecimal> colResult = sumResult.get(col);
        
        // 获取ora库的值
        BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
        
        // 获取rlcms_pv1的值
        BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
        
        // 比较值是否相等
        return oraValue.compareTo(rlcmsPv1Value) == 0;
    }

    @Override
    public BigDecimal diff(TableInfo tableInfo,String col) {
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            return BigDecimal.ZERO;
        }
        
        Map<String, BigDecimal> colResult = sumResult.get(col);
        
        // 获取ora库的值
        BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
        
        // 获取rlcms_pv1的值
        BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
        
        // 返回差异值
        return oraValue.subtract(rlcmsPv1Value);
    }

    @Override
    public String diffDesc(TableInfo tableInfo,String col) {
        BigDecimal diff = diff(tableInfo, col);
        if (diff.compareTo(BigDecimal.ZERO) == 0) {
            return "公式6验证通过：ora = rlcms_pv1";
        } else {
            Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
            Map<String, BigDecimal> colResult = sumResult.get(col);
            
            BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
            BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
            
            return String.format("公式6验证失败：ora(%s) != rlcms_pv1(%s)，差异值: %s",
                    oraValue, rlcmsPv1Value, diff);
        }
    }
} 
