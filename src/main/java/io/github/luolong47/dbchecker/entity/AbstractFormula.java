package io.github.luolong47.dbchecker.entity;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public abstract class AbstractFormula implements Formula {
    
    protected BigDecimal getValueOrZero(Map<String, BigDecimal> colResult, String key) {
        return colResult.getOrDefault(key, BigDecimal.ZERO);
    }
    
    protected Map<String, BigDecimal> getColumnResult(TableInfo tableInfo, String col) {
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            return null;
        }
        return sumResult.get(col);
    }
    
    protected boolean checkSumResultExists(TableInfo tableInfo, String col, String formulaName) {
        if (tableInfo.getSumResult() == null || !tableInfo.getSumResult().containsKey(col)) {
            log.warn("表 [{}] 列 [{}] 没有求和结果，无法验证{}", tableInfo.getTableName(), col, formulaName);
            return false;
        }
        return true;
    }
    
    protected String getSuccessMessage(String formula) {
        return formula + "验证通过：" + getDesc();
    }
    
    /**
     * 计算基准值与多个比较值之间的最大差异值
     * 
     * @param baseValue 基准值
     * @param valuesToCompare 需要比较的多个值
     * @return 最大差异值
     */
    protected BigDecimal calculateMaxDifference(BigDecimal baseValue, BigDecimal... valuesToCompare) {
        BigDecimal maxDiff = BigDecimal.ZERO;
        
        for (BigDecimal value : valuesToCompare) {
            BigDecimal diff = baseValue.subtract(value).abs();
            if (diff.compareTo(maxDiff) > 0) {
                maxDiff = diff;
            }
        }
        
        return maxDiff;
    }
    
    protected abstract boolean compareValues(Map<String, BigDecimal> colResult);
    
    protected abstract BigDecimal calculateDiff(Map<String, BigDecimal> colResult);
    
    protected abstract String getFailureMessage(Map<String, BigDecimal> colResult, BigDecimal diff);
    
    @Override
    public boolean result(TableInfo tableInfo, String col) {
        if (!checkSumResultExists(tableInfo, col, getDesc())) {
            return false;
        }
        
        Map<String, BigDecimal> colResult = getColumnResult(tableInfo, col);
        return compareValues(colResult);
    }
    
    @Override
    public BigDecimal diff(TableInfo tableInfo, String col) {
        Map<String, BigDecimal> colResult = getColumnResult(tableInfo, col);
        if (colResult == null) {
            return BigDecimal.ZERO;
        }
        
        return calculateDiff(colResult);
    }
    
    @Override
    public String diffDesc(TableInfo tableInfo, String col) {
        BigDecimal diff = diff(tableInfo, col);
        if (diff.compareTo(BigDecimal.ZERO) == 0) {
            return getSuccessMessage(getDesc().split(":")[0]);
        } else {
            Map<String, BigDecimal> colResult = getColumnResult(tableInfo, col);
            return getFailureMessage(colResult, diff);
        }
    }
} 