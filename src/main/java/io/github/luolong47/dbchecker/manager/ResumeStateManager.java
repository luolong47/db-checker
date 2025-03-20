package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.File;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 断点续跑状态管理器
 */
@Slf4j
@Component
public class ResumeStateManager {

    // 存储断点续跑状态
    private final Set<String> processedDatabases = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Set<String> processedTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // 缓存数据库处理状态
    private final Map<String, Boolean> databaseProcessingCache = new ConcurrentHashMap<>();
    // 缓存表处理状态
    private final Map<String, Boolean> tableProcessingCache = new ConcurrentHashMap<>();
    // 缓存重跑数据库列表
    private final Map<String, Set<String>> rerunDatabasesCache = new ConcurrentHashMap<>();

    // 引入配置类
    private final DbConfig config;

    public ResumeStateManager(DbConfig config) {
        this.config = config;
    }

    /**
     * 获取断点续跑状态文件路径
     */
    private File getResumeFilePath() {
        if (StrUtil.isNotEmpty(config.getResumeFile())) {
            return new File(config.getResumeFile());
        }
        return new File(config.getExportDirectory(), "/resume_state.json");
    }

    /**
     * 初始化断点续跑状态
     */
    public void initResumeState(String runMode) {
        // 清空当前状态，准备重新加载
        processedDatabases.clear();
        processedTables.clear();

        // 只在断点续跑模式下加载状态
        if ("RESUME".equalsIgnoreCase(runMode)) {
            try {
                File file = getResumeFilePath();
                if (file.exists()) {
                    log.info("检测到断点续跑文件: {}", getResumeFilePath());

                    // 读取JSON文件
                    String jsonStr = FileUtil.readUtf8String(file);
                    JSONObject state = JSONUtil.parseObj(jsonStr);

                    // 恢复已处理的数据库列表
                    JSONArray databasesArray = state.getJSONArray("databases");
                    if (databasesArray != null) {
                        for (int i = 0; i < databasesArray.size(); i++) {
                            processedDatabases.add(databasesArray.getStr(i));
                        }
                    }

                    // 恢复已处理的表列表
                    JSONArray tablesArray = state.getJSONArray("tables");
                    if (tablesArray != null) {
                        for (int i = 0; i < tablesArray.size(); i++) {
                            processedTables.add(tablesArray.getStr(i));
                        }
                    }

                    log.info("成功从断点续跑文件恢复状态，已处理数据库: {}，已处理表: {}",
                        processedDatabases.size(), processedTables.size());
                } else {
                    log.info("没有找到断点续跑文件，将从头开始处理");
                }
            } catch (Exception e) {
                log.error("读取断点续跑文件时出错: {}", e.getMessage(), e);
            }
        } else if ("FULL".equalsIgnoreCase(runMode)) {
            log.info("当前为全量处理模式，忽略断点续跑文件");
        } else {
            log.info("当前为自定义处理模式: {}", runMode);
        }
    }

    /**
     * 保存断点续跑状态
     */
    public void saveResumeState(Map<String, TableInfo> tableInfoMap) {
        try {
            JSONObject json = new JSONObject();
            // 保存已处理的数据库和表列表
            json.set("databases", processedDatabases);
            json.set("tables", processedTables);
            json.set("lastUpdated", DateUtil.now());

            // 序列化表信息
            if (!tableInfoMap.isEmpty()) {
                Map<String, Object> serializedTableInfo = new HashMap<>();

                // 遍历并序列化每个表的元数据
                for (Map.Entry<String, TableInfo> entry : tableInfoMap.entrySet()) {
                    Map<String, Object> tableData = new HashMap<>();
                    TableInfo metaInfo = entry.getValue();

                    // 保存数据源列表
                    tableData.put("dataSources", metaInfo.getDataSources());

                    // 保存记录数
                    tableData.put("recordCounts", metaInfo.getRecordCounts());

                    // 保存金额字段
                    tableData.put("moneyFields", new ArrayList<>(metaInfo.getMoneyFields()));

                    // 保存金额SUM值
                    Map<String, Map<String, String>> moneySumsStr = convertMoneySumsToString(metaInfo.getAllMoneySums());
                    tableData.put("moneySums", moneySumsStr);

                    serializedTableInfo.put(entry.getKey(), tableData);
                }

                json.set("tableInfo", serializedTableInfo);
            }

            // 写入文件
            FileUtil.writeUtf8String(json.toString(), getResumeFilePath());
            log.info("已保存断点续跑状态到{}，包含{}个数据库和{}个表的信息",
                getResumeFilePath().getCanonicalPath(), processedDatabases.size(), processedTables.size());
        } catch (Exception e) {
            log.error("保存断点续跑状态出错: {}", e.getMessage(), e);
        }
    }

    /**
     * 将BigDecimal类型的金额SUM值转换为字符串格式
     *
     * @param moneySums 金额SUM值映射
     * @return 转换后的字符串映射
     */
    private Map<String, Map<String, String>> convertMoneySumsToString(Map<String, Map<String, BigDecimal>> moneySums) {
        Map<String, Map<String, String>> moneySumsStr = new HashMap<>();
        for (Map.Entry<String, Map<String, BigDecimal>> dsEntry : moneySums.entrySet()) {
            Map<String, String> fieldSums = new HashMap<>();
            for (Map.Entry<String, BigDecimal> sumEntry : dsEntry.getValue().entrySet()) {
                if (sumEntry.getValue() != null) {
                    fieldSums.put(sumEntry.getKey(), sumEntry.getValue().toString());
                }
            }
            moneySumsStr.put(dsEntry.getKey(), fieldSums);
        }
        return moneySumsStr;
    }

    /**
     * 过滤数据库列表，根据运行模式和已处理状态决定需要处理的数据库
     */
    public Map<String, JdbcTemplate> filterDatabases(Map<String, JdbcTemplate> allDatabases,
                                                     String runMode, String rerunDatabases) {
        // 全量重跑模式，返回所有数据库
        if ("FULL".equalsIgnoreCase(runMode)) {
            return allDatabases;
        }

        // 指定库重跑模式
        if (!StrUtil.isEmpty(rerunDatabases)) {
            return Arrays.stream(rerunDatabases.split(","))
                .map(String::trim)
                .filter(allDatabases::containsKey)
                .peek(dbName -> {
                    if (!allDatabases.containsKey(dbName)) {
                        log.warn("未找到指定的数据库[{}]，将被忽略", dbName);
                    }
                })
                .collect(Collectors.toMap(
                    dbName -> dbName,
                    allDatabases::get,
                    (v1, v2) -> v1, // 处理键冲突
                    LinkedHashMap::new // 保持顺序
                ));
        }

        // 断点续跑模式，只返回未处理的数据库
        return allDatabases.entrySet().stream()
            .filter(entry -> !processedDatabases.contains(entry.getKey()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (v1, v2) -> v1,
                LinkedHashMap::new
            ));
    }

    /**
     * 检查数据库是否已处理
     */
    public boolean isDatabaseProcessed(String dataSourceName) {
        return processedDatabases.contains(dataSourceName);
    }

    /**
     * 检查数据库是否应该处理（根据运行模式和已处理状态）
     */
    public boolean shouldProcessDatabase(String dataSourceName, String runMode, String rerunDatabases) {
        if ("FULL".equalsIgnoreCase(runMode)) {
            return true;
        }

        String cacheKey = dataSourceName + ":" + runMode + ":" + rerunDatabases;
        return databaseProcessingCache.computeIfAbsent(cacheKey, k -> {
            if (!StrUtil.isEmpty(rerunDatabases)) {
                Set<String> rerunSet = getRerunDatabasesSet(rerunDatabases);
                return rerunSet.contains(dataSourceName.toLowerCase()) || !processedDatabases.contains(dataSourceName);
            }
            return !processedDatabases.contains(dataSourceName);
        });
    }

    /**
     * 检查表是否应该处理（根据运行模式和已处理状态）
     */
    public boolean shouldProcessTable(String tableName, String dataSourceName, String runMode, String rerunDatabases) {
        if ("FULL".equalsIgnoreCase(runMode)) {
            return true;
        }

        String cacheKey = tableName + ":" + dataSourceName + ":" + runMode + ":" + rerunDatabases;
        return tableProcessingCache.computeIfAbsent(cacheKey, k -> {
            String tableKey = StrUtil.format("{}|{}", dataSourceName, tableName);
            if (!StrUtil.isEmpty(rerunDatabases)) {
                Set<String> rerunSet = getRerunDatabasesSet(rerunDatabases);
                return rerunSet.contains(dataSourceName.toLowerCase()) || !processedTables.contains(tableKey);
            }
            return !processedTables.contains(tableKey);
        });
    }

    /**
     * 获取重跑数据库集合（带缓存）
     */
    private Set<String> getRerunDatabasesSet(String rerunDatabases) {
        return rerunDatabasesCache.computeIfAbsent(rerunDatabases, k ->
            Arrays.stream(rerunDatabases.split(","))
                .map(String::trim)
                .map(String::toLowerCase)
                .collect(Collectors.toSet())
        );
    }

    /**
     * 标记数据库为已处理
     */
    public void markDatabaseProcessed(String dataSourceName) {
        processedDatabases.add(dataSourceName);
        // 清除相关缓存
        databaseProcessingCache.keySet().stream()
            .filter(key -> key.startsWith(dataSourceName + ":"))
            .forEach(databaseProcessingCache::remove);
    }

    /**
     * 标记表为已处理
     */
    public void markTableProcessed(String tableName, String dataSourceName) {
        String tableKey = StrUtil.format("{}|{}", dataSourceName, tableName);
        processedTables.add(tableKey);
        // 清除相关缓存
        tableProcessingCache.keySet().stream()
            .filter(key -> key.startsWith(tableName + ":" + dataSourceName + ":"))
            .forEach(tableProcessingCache::remove);
    }
} 