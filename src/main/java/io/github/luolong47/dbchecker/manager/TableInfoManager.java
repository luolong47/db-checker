package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表信息管理器
 */
@Slf4j
@Component
@Data
public class TableInfoManager {

    // 存储所有表的元数据信息，使用线程安全的ConcurrentHashMap
    private final Map<String, TableInfo> tableInfoMap = new ConcurrentHashMap<>();

    /**
     * a获取表数量
     */
    public int getTableCount() {
        return tableInfoMap.size();
    }

    /**
     * 清空表信息
     */
    public void clearTableInfo() {
        tableInfoMap.clear();
    }

    /**
     * 判断是否有表信息
     */
    public boolean hasTableInfo() {
        return !tableInfoMap.isEmpty();
    }

    /**
     * 将表信息列表添加到映射中，使用表名作为唯一标识
     */
    public void addTableInfoList(List<TableInfo> tables, String sourceName) {
        log.info("开始处理{}的{}个表信息...", sourceName, tables.size());

        for (TableInfo table : tables) {
            String key = table.getTableName();

            // 使用ConcurrentHashMap的computeIfAbsent保证线程安全地获取或创建TableInfo对象
            TableInfo metaInfo = tableInfoMap.computeIfAbsent(key, TableInfo::new);

            // 添加数据源和记录数 - 这些操作在TableInfo内部使用ConcurrentHashMap，已线程安全
            metaInfo.addDataSource(sourceName);
            metaInfo.setRecordCount(sourceName, table.getRecordCount());
            metaInfo.setRecordCountAll(sourceName, table.getRecordCountAll(sourceName));

            // 添加金额字段
            if (metaInfo.getMoneyFields().isEmpty() && !table.getMoneyFields().isEmpty()) {
                try {
                    // 使用非阻塞操作添加金额字段
                    for (String moneyField : table.getMoneyFields()) {
                        metaInfo.addMoneyField(moneyField);
                    }

                    if (!table.getMoneyFields().isEmpty()) {
                        log.debug("表 {} 发现{}个金额字段: {}",
                            table.getTableName(),
                            table.getMoneyFields().size(),
                            StrUtil.join(", ", table.getMoneyFields()));
                    }
                } catch (Exception e) {
                    log.error("处理表 {} 的金额字段时出错: {}", table.getTableName(), e.getMessage(), e);
                }
            }

            // 添加求和结果
            if (!table.getMoneySums().isEmpty()) {
                // 使用TableInfo内部的线程安全方法添加金额字段的SUM值
                for (Map.Entry<String, BigDecimal> entry : table.getMoneySums().entrySet()) {
                    String fieldName = entry.getKey();
                    BigDecimal sumValue = entry.getValue();
                    metaInfo.setMoneySum(sourceName, fieldName, sumValue);
                    log.debug("表 {} 字段 {} 在数据源 {} 的SUM值为: {}",
                        table.getTableName(), fieldName, sourceName, sumValue);
                }
            }

            // 添加无条件求和结果
            Map<String, Map<String, BigDecimal>> allMoneySumsAll = table.getAllMoneySumsAll();
            if (allMoneySumsAll != null && !allMoneySumsAll.isEmpty()) {
                Map<String, BigDecimal> fieldSumsAll = allMoneySumsAll.get(sourceName);
                if (fieldSumsAll != null && !fieldSumsAll.isEmpty()) {
                    for (Map.Entry<String, BigDecimal> entry : fieldSumsAll.entrySet()) {
                        String fieldName = entry.getKey();
                        BigDecimal sumValueAll = entry.getValue();
                        metaInfo.setMoneySumAll(sourceName, fieldName, sumValueAll);
                        log.debug("复制表[{}]字段[{}]在数据源[{}]中的无条件SUM值: {}",
                            table.getTableName(), fieldName, sourceName, sumValueAll);
                    }
                }
            }
        }

        log.info("{}的表信息处理完成", sourceName);
    }
} 