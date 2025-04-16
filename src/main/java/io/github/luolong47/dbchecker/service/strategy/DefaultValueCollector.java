package io.github.luolong47.dbchecker.service.strategy;

import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.RequiredArgsConstructor;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 默认值收集器实现
 */
@RequiredArgsConstructor
public class DefaultValueCollector implements ValueCollector {
    // 缓存求和值
    private final Map<String, Map<String, BigDecimal>> sumValuesCache = new ConcurrentHashMap<>();
    // 缓存计数值
    private final Map<String, Map<String, Long>> countValuesCache = new ConcurrentHashMap<>();

    private final String[] dataSources;
    private final DbConfig dbConfig;

    @Override
    public Map<String, BigDecimal> collectSumValues(MoneyFieldSumInfo info) {
        String cacheKey = info.getTableName() + ":" + info.getSumField();
        return sumValuesCache.computeIfAbsent(cacheKey, k -> {
            Map<String, BigDecimal> values = new HashMap<>();
            for (String ds : dataSources) {
                values.put(ds, info.getSumValueByDataSource(ds));
            }
            return values;
        });
    }

    @Override
    public Map<String, Long> collectCountValues(MoneyFieldSumInfo info) {
        String cacheKey = info.getTableName() + ":_COUNT";
        return countValuesCache.computeIfAbsent(cacheKey, k -> {
            Map<String, Long> values = new HashMap<>();
            for (String ds : dataSources) {
                values.put(ds, info.getCountValueByDataSource(ds));
            }
            return values;
        });
    }

    @Override
    public boolean isApproximatelyEqual(BigDecimal a, BigDecimal b) {
        // 检查是否启用求和功能
        if (!dbConfig.getSum().getEnable()) {
            return false;
        }
        
        // 获取配置的精度
        int scale = dbConfig.getSum().getScale();
        
        return Optional.ofNullable(a)
            .flatMap(valueA -> Optional.ofNullable(b)
                .map(valueB -> {
                    // 根据配置的精度处理小数位
                    BigDecimal aScaled = valueA.setScale(scale, RoundingMode.HALF_UP);
                    BigDecimal bScaled = valueB.setScale(scale, RoundingMode.HALF_UP);
                    
                    // 比较处理后的值是否完全相等
                    return aScaled.equals(bScaled);
                }))
            .orElse(false);
    }

    @Override
    public boolean areAllValuesEqual(BigDecimal... values) {
        if (values.length <= 1) {
            return true;
        }

        BigDecimal reference = values[0];
        return Arrays.stream(values)
            .skip(1)
            .allMatch(value -> isApproximatelyEqual(reference, value));
    }

    @Override
    public boolean areAllCountValuesEqual(Collection<Long> values) {
        return values.stream().distinct().count() <= 1;
    }
} 