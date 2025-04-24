package io.github.luolong47.dbchecker.entity;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.Map;

@Slf4j
public class Formula1 implements Formula {

    @Override
    public String getDesc() {
        return StrUtil.format("公式1: ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3");
    }

    @Override
    public boolean result(TableInfo tableInfo,String col) {
        Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
        if (sumResult == null || !sumResult.containsKey(col)) {
            log.warn("表 [{}] 列 [{}] 没有求和结果，无法验证公式1", tableInfo.getTableName(), col);
            return false;
        }
        
        Map<String, BigDecimal> colResult = sumResult.get(col);
        
        // 获取ora库的值
        BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
        
        // 获取rlcms_pv1、rlcms_pv2、rlcms_pv3的值总和
        BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
        BigDecimal rlcmsPv2Value = colResult.getOrDefault("rlcms-pv2", BigDecimal.ZERO);
        BigDecimal rlcmsPv3Value = colResult.getOrDefault("rlcms-pv3", BigDecimal.ZERO);
        
        // 计算总和
        BigDecimal sum = rlcmsPv1Value.add(rlcmsPv2Value).add(rlcmsPv3Value);
        
        // 比较值是否相等（可能需要容忍一定误差）
        return oraValue.compareTo(sum) == 0;
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
        
        // 获取rlcms_pv1、rlcms_pv2、rlcms_pv3的值总和
        BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
        BigDecimal rlcmsPv2Value = colResult.getOrDefault("rlcms-pv2", BigDecimal.ZERO);
        BigDecimal rlcmsPv3Value = colResult.getOrDefault("rlcms-pv3", BigDecimal.ZERO);
        
        // 计算总和
        BigDecimal sum = rlcmsPv1Value.add(rlcmsPv2Value).add(rlcmsPv3Value);
        
        // 返回差异值
        return oraValue.subtract(sum);
    }

    @Override
    public String diffDesc(TableInfo tableInfo,String col) {
        BigDecimal diff = diff(tableInfo, col);
        if (diff.compareTo(BigDecimal.ZERO) == 0) {
            return StrUtil.format("公式1验证通过：ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3");
        } else {
            Map<String, Map<String, BigDecimal>> sumResult = tableInfo.getSumResult();
            Map<String, BigDecimal> colResult = sumResult.get(col);
            
            BigDecimal oraValue = colResult.getOrDefault("ora", BigDecimal.ZERO);
            BigDecimal rlcmsPv1Value = colResult.getOrDefault("rlcms-pv1", BigDecimal.ZERO);
            BigDecimal rlcmsPv2Value = colResult.getOrDefault("rlcms-pv2", BigDecimal.ZERO);
            BigDecimal rlcmsPv3Value = colResult.getOrDefault("rlcms-pv3", BigDecimal.ZERO);
            
            return StrUtil.format("公式1验证失败：ora({}) != rlcms_pv1({}) + rlcms_pv2({}) + rlcms_pv3({})，差异值: {}",
                    oraValue, rlcmsPv1Value, rlcmsPv2Value, rlcmsPv3Value, diff);
        }
    }
}
