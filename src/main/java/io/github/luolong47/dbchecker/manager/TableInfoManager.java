package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 表信息管理器
 */
@Slf4j
@Component
public class TableInfoManager {

    // 存储所有表的元数据信息
    private final Map<String, TableInfo> tableInfoMap = new HashMap<>();

    /**
     * 获取表信息映射
     */
    public Map<String, TableInfo> getTableInfoMap() {
        return tableInfoMap;
    }

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
     * 获取指定表的信息
     */
    public TableInfo getTableInfo(String tableName) {
        return tableInfoMap.get(tableName);
    }

    /**
     * 添加表信息
     */
    public void addTableInfo(TableInfo tableInfo) {
        if (tableInfo != null) {
            tableInfoMap.put(tableInfo.getTableName(), tableInfo);
        }
    }

    /**
     * 将表信息列表添加到映射中，使用表名作为唯一标识
     */
    public void addTableInfoList(List<TableInfo> tables, String sourceName) {
        log.info("开始处理{}的{}个表信息...", sourceName, tables.size());

        tables.forEach(table -> {
            String key = table.getTableName();
            TableInfo metaInfo = tableInfoMap.computeIfAbsent(key, TableInfo::new);

            // 添加数据源和记录数
            metaInfo.addDataSource(sourceName);
            metaInfo.setRecordCount(sourceName, table.getRecordCount());

            // 添加金额字段（从TableInfo直接获取）
            if (metaInfo.getMoneyFields().isEmpty() && !table.getMoneyFields().isEmpty()) {
                try {
                    // 使用Stream API复制金额字段
                    table.getMoneyFields().forEach(metaInfo::addMoneyField);

                    if (!table.getMoneyFields().isEmpty()) {
                        log.info("表 {} 发现{}个金额字段: {}",
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
                // 使用Stream API添加金额字段的SUM值
                table.getMoneySums().forEach((fieldName, sumValue) -> {
                    metaInfo.setMoneySum(sourceName, fieldName, sumValue);
                    log.debug("表 {} 字段 {} 在数据源 {} 的SUM值为: {}",
                        table.getTableName(), fieldName, sourceName, sumValue);
                });
            }
        });

        log.info("{}的表信息处理完成", sourceName);
    }
} 