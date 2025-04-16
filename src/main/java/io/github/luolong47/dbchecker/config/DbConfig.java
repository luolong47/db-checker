package io.github.luolong47.dbchecker.config;

import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据库查询WHERE条件配置
 */
@Data
@Slf4j
@ConfigurationProperties(prefix = "db")
public class DbConfig {

    private Map<String, Map<String, String>> where = new HashMap<>();

    /**
     * 包含配置
     */
    @NestedConfigurationProperty
    private Include include = new Include();

    /**
     * 导出配置
     */
    @NestedConfigurationProperty
    private Export export = new Export();

    /**
     * 运行模式配置
     */
    @NestedConfigurationProperty
    private Run run = new Run();

    /**
     * 重跑配置
     */
    @NestedConfigurationProperty
    private Rerun rerun = new Rerun();

    /**
     * 断点续跑配置
     */
    @NestedConfigurationProperty
    private Resume resume = new Resume();

    /**
     * 公式配置
     */
    @NestedConfigurationProperty
    private Formula formula = new Formula();

    /**
     * SQL提示缓存: key=dataSourceName:tableName, value=SQL提示
     */
    private final Map<String, String> sqlHintCache = new ConcurrentHashMap<>();
    /**
     * 从节点查询配置
     */
    @NestedConfigurationProperty
    private SlaveQuery slaveQuery = new SlaveQuery();
    /**
     * WHERE条件缓存: key=dataSourceName:tableName, value=条件语句(或null)
     */
    private final Map<String, String> conditionCache = new ConcurrentHashMap<>();
    /**
     * Schema列表缓存
     */
    private final Map<String, List<String>> schemasCache = new ConcurrentHashMap<>();
    /**
     * 表名列表缓存
     */
    private final Map<String, List<String>> tablesCache = new ConcurrentHashMap<>();
    /**
     * SQL提示配置
     */
    @NestedConfigurationProperty
    private Hints hints = new Hints();

    /**
     * 求和配置
     */
    @NestedConfigurationProperty
    private Sum sum = new Sum();

    /**
     * 初始化时记录已加载的条件
     */
    @PostConstruct
    public void init() {
        if (where == null || where.isEmpty()) {
            log.warn("未加载任何db.where条件配置");
        } else {
            log.debug("已加载{}个数据源的条件配置:", where.size());
            for (Map.Entry<String, Map<String, String>> entry : where.entrySet()) {
                String dataSource = entry.getKey();
                Map<String, String> tableConditions = entry.getValue();
                log.debug("数据源[{}]条件配置: {}", dataSource, tableConditions);
            }
        }

        // 记录其他配置信息
        log.debug("配置信息 - 包含Schema: {}", include.getSchemas());
        log.debug("配置信息 - 包含表: {}", include.getTables());
        log.debug("配置信息 - 导出目录: {}", export.getDirectory());
        log.debug("配置信息 - 运行模式: {}", run.getMode());
        log.debug("配置信息 - 重跑数据库: {}", rerun.getDatabases());
        log.debug("配置信息 - 断点续跑文件: {}", resume.getFile());
        log.debug("配置信息 - 公式1适用表: {}", formula.getFormula1());
        log.debug("配置信息 - 公式2适用表: {}", formula.getFormula2());
        log.debug("配置信息 - 公式3适用表: {}", formula.getFormula3());
        log.debug("配置信息 - 公式4适用表: {}", formula.getFormula4());
        log.debug("配置信息 - 公式5适用表: {}", formula.getFormula5());
        log.debug("配置信息 - 公式6适用表: {}", formula.getFormula6());
        log.debug("配置信息 - 从节点查询表: {}", slaveQuery.getTables());

        // 记录SQL提示配置
        log.debug("配置信息 - SQL提示类型映射: {}", hints.getType());
        log.debug("配置信息 - SQL提示表映射: {}", hints.getTable());
        log.debug("配置信息 - SQL提示语句: {}", hints.getSql());

        // 预处理数据源类型映射和表类型映射，以优化getSqlHint的性能
        initTypeMap();

        // 预处理WHERE条件缓存，以优化getCondition的性能
        initConditionCache();
    }

    /**
     * 初始化条件缓存
     */
    private void initConditionCache() {
        int cacheCount = 0;

        if (where != null && !where.isEmpty()) {
            for (Map.Entry<String, Map<String, String>> dsEntry : where.entrySet()) {
                String dataSourceName = dsEntry.getKey().toLowerCase();
                Map<String, String> tableConditions = dsEntry.getValue();

                if (tableConditions != null && !tableConditions.isEmpty()) {
                    for (Map.Entry<String, String> tblEntry : tableConditions.entrySet()) {
                        String tableName = tblEntry.getKey().toLowerCase();
                        String condition = tblEntry.getValue();

                        // 生成缓存键
                        String cacheKey = dataSourceName + ":" + tableName;

                        // 存入缓存
                        conditionCache.put(cacheKey, condition);
                        cacheCount++;
                        log.debug("预计算WHERE条件缓存: [{}] -> [{}]", cacheKey, condition);
                    }
                }
            }
        }

        log.debug("WHERE条件缓存初始化完成: {} 个缓存项", cacheCount);
    }

    /**
     * 初始化类型映射
     */
    private void initTypeMap() {
        // 构建数据源类型映射 (作为局部变量)
        Map<String, String> dataSourceTypeMap = new HashMap<>();
        // 表类型映射 (作为局部变量)
        Map<String, Boolean> tableTypeMap = new HashMap<>();

        Map<String, String> typeConfig = hints.getType();
        if (typeConfig != null) {
            for (Map.Entry<String, String> entry : typeConfig.entrySet()) {
                String typeKey = entry.getKey();
                String dataSources = entry.getValue();

                // 将数据源与类型关联
                if (StrUtil.isNotEmpty(dataSources)) {
                    for (String ds : dataSources.split(",")) {
                        String trimmedDs = ds.trim().toLowerCase();
                        dataSourceTypeMap.put(trimmedDs, typeKey);
                        log.debug("初始化数据源类型映射: [{}] -> [{}]", trimmedDs, typeKey);
                    }
                }
            }
        }

        // 构建表类型映射
        Map<String, String> tableConfig = hints.getTable();
        if (tableConfig != null) {
            for (Map.Entry<String, String> entry : tableConfig.entrySet()) {
                String typeKey = entry.getKey();
                String tables = entry.getValue();

                // 将表与类型关联
                if (StrUtil.isNotEmpty(tables)) {
                    for (String tbl : tables.split(",")) {
                        String trimmedTbl = tbl.trim().toLowerCase();
                        String key = typeKey + ":" + trimmedTbl;
                        tableTypeMap.put(key, Boolean.TRUE);
                        log.debug("初始化表类型映射: [{}] -> TRUE", key);
                    }
                }
            }
        }

        // 预先计算所有可能的SQL提示并填充缓存
        Map<String, String> sqlConfig = hints.getSql();
        int cacheCount = 0;

        if (typeConfig != null && tableConfig != null && sqlConfig != null) {
            // 遍历所有数据源
            for (Map.Entry<String, String> dsEntry : typeConfig.entrySet()) {
                String typeKey = dsEntry.getKey();
                String dataSources = dsEntry.getValue();

                // 获取该类型的SQL提示
                String sqlHint = sqlConfig.getOrDefault(typeKey, "");

                // 获取该类型下的所有表
                String tables = tableConfig.getOrDefault(typeKey, "");

                if (StrUtil.isNotEmpty(dataSources) && StrUtil.isNotEmpty(tables)) {
                    // 为每个数据源和表组合预计算SQL提示
                    for (String ds : dataSources.split(",")) {
                        String trimmedDs = ds.trim().toLowerCase();

                        for (String tbl : tables.split(",")) {
                            String trimmedTbl = tbl.trim().toLowerCase();

                            // 生成缓存键
                            String cacheKey = trimmedDs + ":" + trimmedTbl;

                            // 存入缓存
                            sqlHintCache.put(cacheKey, sqlHint);
                            cacheCount++;
                            log.debug("预计算SQL提示缓存: [{}] -> [{}]", cacheKey, sqlHint);
                        }
                    }
                }
            }
        }

        log.info("SQL提示类型映射初始化完成: {} 个数据源映射, {} 个表映射, {} 个SQL提示缓存",
            dataSourceTypeMap.size(), tableTypeMap.size(), cacheCount);
    }

    /**
     * 获取包含的schema列表
     */
    public List<String> getIncludeSchemasList() {
        return schemasCache.computeIfAbsent("schemas", k -> {
            String schemas = include.getSchemas();
            if (StrUtil.isEmpty(schemas)) {
                return Collections.emptyList();
            }
            return Arrays.asList(schemas.split(","));
        });
    }

    /**
     * 获取包含的表名列表
     */
    public List<String> getIncludeTablesList() {
        return tablesCache.computeIfAbsent("tables", k -> {
            String tables = include.getTables();
            if (StrUtil.isEmpty(tables)) {
                return Collections.emptyList();
            }
            return Arrays.asList(tables.split(","));
        });
    }

    /**
     * 获取包含的schema列表（原始字符串）
     */
    public String getIncludeSchemas() {
        return include.getSchemas();
    }

    /**
     * 获取包含的表名列表（原始字符串）
     */
    public String getIncludeTables() {
        return include.getTables();
    }

    /**
     * 获取导出目录
     */
    public String getExportDirectory() {
        return export.getDirectory();
    }

    /**
     * 获取运行模式
     */
    public String getRunMode() {
        return run.getMode();
    }

    /**
     * 获取重跑数据库列表
     */
    public String getRerunDatabases() {
        return rerun.getDatabases();
    }

    /**
     * 获取断点续跑文件路径
     */
    public String getResumeFile() {
        return resume.getFile();
    }

    /**
     * 获取公式1适用表
     */
    public String getFormula1() {
        return formula.getFormula1();
    }

    /**
     * 获取公式2适用表
     */
    public String getFormula2() {
        return formula.getFormula2();
    }

    /**
     * 获取公式3适用表
     */
    public String getFormula3() {
        return formula.getFormula3();
    }

    /**
     * 获取公式4适用表
     */
    public String getFormula4() {
        return formula.getFormula4();
    }

    /**
     * 获取公式5适用表
     */
    public String getFormula5() {
        return formula.getFormula5();
    }

    /**
     * 获取公式6适用表
     */
    public String getFormula6() {
        return formula.getFormula6();
    }

    /**
     * 获取指定数据源和表的WHERE条件，优化版本 - 时间复杂度O(1)
     * 使用预处理的缓存，避免重复查找
     *
     * @param dataSourceName 数据源名称
     * @param tableName      表名
     * @return WHERE条件（不含WHERE关键字）或null
     */
    public String getCondition(String dataSourceName, String tableName) {
        if (StrUtil.isEmpty(dataSourceName) || StrUtil.isEmpty(tableName)) {
            return null;
        }

        // 生成缓存键
        String cacheKey = dataSourceName.toLowerCase() + ":" + tableName.toLowerCase();

        // 从缓存中获取结果
        String condition = conditionCache.get(cacheKey);

        // 记录日志
        if (condition != null) {
            log.debug("从缓存中找到表 [{}] 的条件: {}", tableName, condition);
        } else {
            log.debug("未找到数据源 [{}] 表 [{}] 的条件配置", dataSourceName, tableName);
        }

        return condition;
    }

    /**
     * 获取SQL提示，优化版本 - 时间复杂度O(1)
     * 使用预处理的映射表和缓存，避免重复计算
     *
     * @param dataSourceName 数据源名称
     * @param tableName      表名
     * @return SQL提示语句或空字符串
     */
    public String getSqlHint(String dataSourceName, String tableName) {
        if (StrUtil.isEmpty(dataSourceName) || StrUtil.isEmpty(tableName)) {
            return "";
        }

        // 生成缓存键
        String cacheKey = dataSourceName.toLowerCase() + ":" + tableName.toLowerCase();

        // 检查缓存
        return sqlHintCache.getOrDefault(cacheKey, "");
    }

    /**
     * 应用SQL提示到SELECT语句
     *
     * @param sql            原始SQL语句
     * @param dataSourceName 数据源名称
     * @param tableName      表名
     * @return 应用了SQL提示的SQL语句
     */
    public String applySqlHint(String sql, String dataSourceName, String tableName) {
        String hint = getSqlHint(dataSourceName, tableName);
        if (StrUtil.isNotEmpty(hint)) {
            // 如果SQL语句包含SELECT，在SELECT后插入提示
            if (StrUtil.containsIgnoreCase(sql, "select")) {
                String newSql = sql.replaceFirst("(?i)select", "select " + hint);
                log.debug("应用SQL提示 - 原SQL: [{}], 新SQL: [{}]", sql, newSql);
                return newSql;
            }
        }
        return sql;
    }

    /**
     * 判断表是否应该使用从节点查询
     *
     * @param tableName 表名
     * @return 是否使用从节点查询
     */
    public boolean shouldUseSlaveQuery(String tableName) {
        if (slaveQuery == null || StrUtil.isEmpty(slaveQuery.getTables())) {
            return false;
        }

        String tables = slaveQuery.getTables();
        return Arrays.stream(tables.split(","))
            .map(String::trim)
            .map(String::toLowerCase)
            .anyMatch(t -> t.equalsIgnoreCase(tableName));
    }

    /**
     * 获取指定表应该使用的数据源名称
     *
     * @param tableName              表名
     * @param originalDataSourceName 原始数据源名称
     * @return 应该使用的数据源名称
     */
    public String getDataSourceToUse(String tableName, String originalDataSourceName) {
        // 如果原始数据源是ora，并且表配置为使用从节点，则返回ora-slave
        if ("ora".equalsIgnoreCase(originalDataSourceName) && shouldUseSlaveQuery(tableName)) {
            return "ora-slave";
        }
        return originalDataSourceName;
    }

    /**
     * 包含配置类
     */
    @Data
    public static class Include {
        /**
         * 要包含的schema列表，使用逗号分隔
         */
        private String schemas;

        /**
         * 要包含的表名列表，使用逗号分隔
         */
        private String tables;
    }

    /**
     * 导出配置类
     */
    @Data
    public static class Export {
        /**
         * 输出文件的目录，默认为当前目录
         */
        private String directory = ".";
    }

    /**
     * 运行模式配置类
     */
    @Data
    public static class Run {
        /**
         * 运行模式：RESUME(断点续跑), FULL(全量重跑), DB_NAME(指定库重跑)
         */
        private String mode = "RESUME";
    }

    /**
     * 重跑配置类
     */
    @Data
    public static class Rerun {
        /**
         * 指定要重跑的数据库，多个用逗号分隔
         */
        private String databases;
    }

    /**
     * 断点续跑配置类
     */
    @Data
    public static class Resume {
        /**
         * 断点续跑状态文件路径
         */
        private String file;
    }

    /**
     * 公式配置类
     */
    @Data
    public static class Formula {
        /**
         * 公式1适用表，使用逗号分隔
         */
        private String formula1;

        /**
         * 公式2适用表，使用逗号分隔
         */
        private String formula2;

        /**
         * 公式3适用表，使用逗号分隔
         */
        private String formula3;

        /**
         * 公式4适用表，使用逗号分隔
         */
        private String formula4;

        /**
         * 公式5适用表，使用逗号分隔
         */
        private String formula5;

        /**
         * 公式6适用表，使用逗号分隔
         */
        private String formula6;
    }

    /**
     * SQL提示配置类
     */
    @Data
    public static class Hints {
        /**
         * 类型配置，键为类型标识(如t1,t2)，值为数据源名称列表
         */
        private Map<String, String> type = new HashMap<>();

        /**
         * 表配置，键为类型标识(如t1,t2)，值为表名列表
         */
        private Map<String, String> table = new HashMap<>();

        /**
         * SQL提示配置，键为类型标识(如t1,t2)，值为SQL提示语句
         */
        private Map<String, String> sql = new HashMap<>();
    }

    /**
     * 从节点查询配置
     */
    @Data
    public static class SlaveQuery {
        /**
         * 要使用从节点查询的表名列表，多个表名使用逗号分隔
         */
        private String tables;
    }

    /**
     * 求和配置
     */
    @Data
    public static class Sum {
        /**
         * 是否启用求和
         */
        private Boolean enable = true;
        
        /**
         * 求和精度（小数位数）
         */
        private Integer scale = 3;
        
        /**
         * 金额字段识别的最小小数位数
         */
        private Integer minDecimalDigits = 1;
    }
} 