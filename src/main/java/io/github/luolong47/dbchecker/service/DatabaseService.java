package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.config.DbWhereConditionConfig;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 数据库服务类，用于测试多数据源
 */
@Slf4j
@Service
public class DatabaseService {

    private final JdbcTemplate oraJdbcTemplate;
    private final JdbcTemplate rlcmsBaseJdbcTemplate;
    private final JdbcTemplate rlcmsPv1JdbcTemplate;
    private final JdbcTemplate rlcmsPv2JdbcTemplate;
    private final JdbcTemplate rlcmsPv3JdbcTemplate;
    private final JdbcTemplate bscopyPv1JdbcTemplate;
    private final JdbcTemplate bscopyPv2JdbcTemplate;
    private final JdbcTemplate bscopyPv3JdbcTemplate;
    private final DbWhereConditionConfig whereConditionConfig;

    // 用户可配置的要包含的schema列表，使用逗号分隔
    @Value("${db.include.schemas:}")
    private String includeSchemas;

    // 用户可配置的要包含的表名列表，使用逗号分隔
    @Value("${db.include.tables:}")
    private String includeTables;

    // 用户可配置的要排除的表名列表，使用逗号分隔
    @Value("${db.exclude.tables:}")
    private String excludeTables;

    // 输出文件的目录，默认为当前目录
    @Value("${db.export.directory:.}")
    private String exportDirectory;

    // 运行模式：RESUME(断点续跑), FULL(全量重跑), DB_NAME(指定库重跑)
    @Value("${db.run.mode:RESUME}")
    private String runMode;

    // 存储断点续跑状态的文件路径
    @Value("${db.resume.file:${db.export.directory}/resume_state.json}")
    private String resumeFile;

    // 指定要重跑的数据库，多个用逗号分隔
    @Value("${db.rerun.databases:}")
    private String rerunDatabases;

    // 存储断点续跑状态
    private final Set<String> processedDatabases = new HashSet<>();
    private final Set<String> processedTables = new HashSet<>();

    public DatabaseService(
            @Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate,
            @Qualifier("rlcmsBaseJdbcTemplate") JdbcTemplate rlcmsBaseJdbcTemplate,
            @Qualifier("rlcmsPv1JdbcTemplate") JdbcTemplate rlcmsPv1JdbcTemplate,
            @Qualifier("rlcmsPv2JdbcTemplate") JdbcTemplate rlcmsPv2JdbcTemplate,
            @Qualifier("rlcmsPv3JdbcTemplate") JdbcTemplate rlcmsPv3JdbcTemplate,
            @Qualifier("bscopyPv1JdbcTemplate") JdbcTemplate bscopyPv1JdbcTemplate,
            @Qualifier("bscopyPv2JdbcTemplate") JdbcTemplate bscopyPv2JdbcTemplate,
            @Qualifier("bscopyPv3JdbcTemplate") JdbcTemplate bscopyPv3JdbcTemplate,
            DbWhereConditionConfig whereConditionConfig) {
        this.oraJdbcTemplate = oraJdbcTemplate;
        this.rlcmsBaseJdbcTemplate = rlcmsBaseJdbcTemplate;
        this.rlcmsPv1JdbcTemplate = rlcmsPv1JdbcTemplate;
        this.rlcmsPv2JdbcTemplate = rlcmsPv2JdbcTemplate;
        this.rlcmsPv3JdbcTemplate = rlcmsPv3JdbcTemplate;
        this.bscopyPv1JdbcTemplate = bscopyPv1JdbcTemplate;
        this.bscopyPv2JdbcTemplate = bscopyPv2JdbcTemplate;
        this.bscopyPv3JdbcTemplate = bscopyPv3JdbcTemplate;
        this.whereConditionConfig = whereConditionConfig;
    }

    /**
     * 初始化断点续跑状态
     */
    @PostConstruct
    public void initResumeState() {
        File file = FileUtil.file(resumeFile);
        if ("RESUME".equalsIgnoreCase(runMode) && file.exists()) {
            try {
                String content = FileUtil.readUtf8String(file);
                JSONObject json = JSONUtil.parseObj(content);
                JSONArray dbs = json.getJSONArray("processedDatabases");
                JSONArray tables = json.getJSONArray("processedTables");

                if (dbs != null) {
                    for (int i = 0; i < dbs.size(); i++) {
                        processedDatabases.add(dbs.getStr(i));
                    }
                }

                if (tables != null) {
                    for (int i = 0; i < tables.size(); i++) {
                        processedTables.add(tables.getStr(i));
                    }
                }

                log.info("已从{}加载断点续跑状态，已处理{}个数据库，{}个表",
                        resumeFile, processedDatabases.size(), processedTables.size());
            } catch (Exception e) {
                log.error("加载断点续跑状态出错: {}", e.getMessage(), e);
                // 出错时重置状态，从头开始
                processedDatabases.clear();
                processedTables.clear();
            }
        } else {
            log.info("运行模式为{}，将{}进行处理",
                    runMode, "RESUME".equalsIgnoreCase(runMode) ? "继续上次未完成的" : "重新");
            processedDatabases.clear();
            processedTables.clear();
        }
    }

    /**
     * 保存断点续跑状态
     */
    private void saveResumeState() {
        try {
            JSONObject json = new JSONObject();
            json.set("processedDatabases", processedDatabases);
            json.set("processedTables", processedTables);
            json.set("lastUpdated", DateUtil.now());

            FileUtil.writeUtf8String(json.toString(), resumeFile);
            log.info("已保存断点续跑状态到{}", resumeFile);
        } catch (Exception e) {
            log.error("保存断点续跑状态出错: {}", e.getMessage(), e);
        }
    }

    /**
     * 检查数据库是否应该处理（根据运行模式和已处理状态）
     *
     * @param dataSourceName 数据源名称
     * @return 是否应该处理
     */
    private boolean shouldProcessDatabase(String dataSourceName) {
        // 如果是全量重跑，总是处理
        if ("FULL".equalsIgnoreCase(runMode)) {
            return true;
        }

        // 如果是指定库重跑，检查是否在指定列表中
        if (!StrUtil.isEmpty(rerunDatabases)) {
            List<String> dbs = StrUtil.split(rerunDatabases, ',');
            for (String db : dbs) {
                if (StrUtil.equalsIgnoreCase(StrUtil.trim(db), dataSourceName)) {
                    return true;
                }
            }

            // 不在指定重跑列表中，且已处理过，则跳过
            if (processedDatabases.contains(dataSourceName)) {
                return false;
            }
        }

        // 如果是断点续跑模式，检查是否已处理
        if ("RESUME".equalsIgnoreCase(runMode)) {
            return !processedDatabases.contains(dataSourceName);
        }

        // 默认处理
        return true;
    }

    /**
     * 检查表是否应该处理（根据运行模式和已处理状态）
     *
     * @param tableName      表名
     * @param schema         表所在的schema
     * @param dataSourceName 数据源名称
     * @return 是否应该处理
     */
    private boolean shouldProcessTable(String tableName, String schema, String dataSourceName) {
        // 标识符：数据源名称 + schema + 表名
        String tableKey = StrUtil.format("{}|{}|{}", dataSourceName, StrUtil.emptyToDefault(schema, ""), tableName);

        // 如果是全量重跑，总是处理
        if (StrUtil.equalsIgnoreCase("FULL", runMode)) {
            return true;
        }

        // 如果是指定库重跑，检查是否在指定列表中
        if (!StrUtil.isEmpty(rerunDatabases)) {
            List<String> dbs = StrUtil.split(rerunDatabases, ',');
            for (String db : dbs) {
                if (StrUtil.equalsIgnoreCase(StrUtil.trim(db), dataSourceName)) {
                    log.info("表[{}]在需要重跑的数据库[{}]中，符合处理条件", tableName, dataSourceName);
                    return true;
                }
            }
        }

        // 如果是断点续跑模式，检查是否已处理
        if ("RESUME".equalsIgnoreCase(runMode)) {
            return !processedTables.contains(tableKey);
        }

        // 默认处理
        return true;
    }

    /**
     * 标记数据库为已处理
     *
     * @param dataSourceName 数据源名称
     */
    private void markDatabaseProcessed(String dataSourceName) {
        processedDatabases.add(dataSourceName);
        saveResumeState();
    }

    /**
     * 标记表为已处理
     *
     * @param tableName      表名
     * @param schema         表所在的schema
     * @param dataSourceName 数据源名称
     */
    private void markTableProcessed(String tableName, String schema, String dataSourceName) {
        // 标识符：数据源名称 + schema + 表名
        String tableKey = StrUtil.format("{}|{}|{}", dataSourceName, StrUtil.emptyToDefault(schema, ""), tableName);
        processedTables.add(tableKey);

        // 每处理10个表保存一次状态，避免频繁IO
        if (processedTables.size() % 10 == 0) {
            saveResumeState();
        }
    }

    /**
     * 从数据源获取表信息，同时获取表的记录数和金额字段的求和
     */
    private List<TableInfo> getTablesInfoFromDataSource(JdbcTemplate jdbcTemplate, String dataSourceName) {
        // 检查是否应该处理该数据库
        if (!shouldProcessDatabase(dataSourceName)) {
            log.info("数据源[{}]已处理或不需要处理，跳过", dataSourceName);
            return Collections.emptyList();
        }

        log.info("开始获取数据源[{}]的表信息", dataSourceName);
        log.info("当前配置 - 包含Schema: [{}], 包含表: [{}], 排除表: [{}]",
                includeSchemas != null ? includeSchemas : "空",
                includeTables != null ? includeTables : "空",
                excludeTables != null ? excludeTables : "空");

        List<TableInfo> tables = new ArrayList<>();
        try (Connection connection = jdbcTemplate.getDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tablesResultSet = metaData.getTables(null, null, "%", new String[]{"TABLE"});

            while (tablesResultSet.next()) {
                String tableName = tablesResultSet.getString("TABLE_NAME");
                // 数字开头的表名要加引号
                if (ReUtil.isMatch("^\\d+.*", tableName)) {
                    tableName = StrUtil.format("\"{}\"", tableName);
                }
                String tableSchema = tablesResultSet.getString("TABLE_SCHEM");

                // 判断是否应该排除该表或已处理过该表
                if (shouldExcludeTable(tableName, tableSchema) ||
                        !shouldProcessTable(tableName, tableSchema, dataSourceName)) {
                    continue;
                }

                TableInfo tableInfo = new TableInfo(tableName, tableSchema != null ? tableSchema : "");
                tableInfo.setDataSourceName(dataSourceName);

                // 获取金额字段并添加到TableInfo中
                try {
                    // 获取列信息
                    try (ResultSet columns = metaData.getColumns(null, tableSchema, tableName, null)) {
                        while (columns.next()) {
                            String columnName = columns.getString("COLUMN_NAME");
                            int dataType = columns.getInt("DATA_TYPE");
                            int decimalDigits = columns.getInt("DECIMAL_DIGITS");

                            // 判断是否为金额字段：数值型且小数位不为0
                            if (isNumericType(dataType) && decimalDigits > 0) {
                                tableInfo.getMoneyFields().add(columnName);
                                log.debug("发现金额字段: {}.{}.{}, 类型: {}, 小数位: {}",
                                        tableSchema, tableName, columnName, dataType, decimalDigits);
                            }
                        }
                    }

                    if (!tableInfo.getMoneyFields().isEmpty()) {
                        log.info("表[{}]中发现{}个金额字段: {}", tableName, tableInfo.getMoneyFields().size(), StrUtil.join(", ", tableInfo.getMoneyFields()));
                    }

                    // 获取表记录数
                    try {
                        // 拼接带Schema的完整表名
                        String fullTableName = StrUtil.isNotEmpty(tableSchema)
                                ? StrUtil.format("{}.{}", tableSchema, tableName)
                                : tableName;

                        // 查询记录数
                        String countSql = StrUtil.format("SELECT COUNT(*) FROM {}", fullTableName);
                        // 应用WHERE条件
                        countSql = whereConditionConfig.applyCondition(countSql, dataSourceName, tableName);
                        log.debug("执行SQL: {}", countSql);
                        Long count = jdbcTemplate.queryForObject(countSql, Long.class);
                        tableInfo.setRecordCount(count != null ? count : 0L);
                        log.info("表[{}]在{}中有{}条记录", tableName, dataSourceName, tableInfo.getRecordCount());

                        // 如果有金额字段，计算它们的SUM
                        if (!tableInfo.getMoneyFields().isEmpty()) {
                            // 构建查询语句，一次查询所有字段的SUM
                            List<String> sumExpressions = tableInfo.getMoneyFields().stream()
                                    .map(field -> StrUtil.format("SUM({}) AS \"{}\"", field, field))
                                    .collect(Collectors.toList());

                            String sumSql = StrUtil.format("SELECT {} FROM {}",
                                    StrUtil.join(", ", sumExpressions),
                                    fullTableName);

                            // 应用WHERE条件
                            sumSql = whereConditionConfig.applyCondition(sumSql, dataSourceName, tableName);

                            log.info("执行批量SUM查询: {}", sumSql);

                            // 执行查询并映射结果
                            Map<String, Object> resultMap = jdbcTemplate.queryForMap(sumSql);
                            for (String fieldName : tableInfo.getMoneyFields()) {
                                Object value = resultMap.get(fieldName);
                                BigDecimal decimalValue = null;
                                if (value instanceof BigDecimal) {
                                    decimalValue = (BigDecimal) value;
                                } else if (value != null) {
                                    try {
                                        decimalValue = new BigDecimal(value.toString());
                                    } catch (NumberFormatException e) {
                                        log.error("无法将 {} 转换为 BigDecimal: {}", value, e.getMessage());
                                    }
                                }
                                tableInfo.getMoneySums().put(fieldName, decimalValue != null ? decimalValue : BigDecimal.ZERO);
                                log.debug("表 {}.{} 字段 {} 的SUM值为: {}", tableSchema, tableName, fieldName, decimalValue);
                            }
                        }

                        // 标记该表为已处理
                        markTableProcessed(tableName, tableSchema, dataSourceName);
                    } catch (Exception e) {
                        log.error("获取表[{}]的记录数或SUM值时出错: {}", tableName, e.getMessage(), e);
                    }
                } catch (SQLException e) {
                    log.error("获取表[{}]的金额字段时出错: {}", tableName, e.getMessage(), e);
                }

                tables.add(tableInfo);
            }

            tablesResultSet.close();

            log.info("从{}获取到{}个非系统表", dataSourceName, tables.size());

            // 标记该数据库为已处理
            markDatabaseProcessed(dataSourceName);

            return tables;
        } catch (SQLException e) {
            log.error("从数据源[{}]获取表信息时出错: {}", dataSourceName, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    /**
     * 判断表是否应该被排除
     * 判断逻辑：
     * 1. 如果表在includeTables中，则一定包含
     * 2. 如果表在excludeTables中，则一定排除
     * 3. 如果schema在includeSchemas中，则包含（除非在excludeTables中）
     * 4. 其他情况都排除
     */
    private boolean shouldExcludeTable(String tableName, String schema) {
        // 1. 首先检查是否在明确包含的表列表中
        if (isInIncludedTables(tableName, schema)) {
            log.debug("表[{}.{}]在包含列表中，将被处理", schema, tableName);
            return false;
        }

        // 2. 检查是否在排除的表列表中
        if (isInExcludedTables(tableName, schema)) {
            log.info("表[{}.{}]在排除列表中，将被排除", schema, tableName);
            return true;
        }

        // 3. 如果schema在包含列表中，则包含该表
        if (isInIncludedSchemas(schema)) {
            log.debug("表[{}.{}]的schema在包含列表中，将被处理", schema, tableName);
            return false;
        }

        // 4. 默认排除
        log.info("表[{}.{}]的schema不在包含列表中，将被排除", schema, tableName);
        return true;
    }

    /**
     * 检查表名是否在包含列表中
     */
    private boolean isInIncludedTables(String tableName, String schema) {
        if (StrUtil.isBlank(includeTables)) {
            return false;
        }

        String fullName = StrUtil.format("{}@{}", tableName, schema);
        List<String> tables = StrUtil.split(includeTables, ',');

        for (String table : tables) {
            if (StrUtil.isEmpty(table)) {
                continue;
            }

            if (StrUtil.equalsIgnoreCase(StrUtil.trim(table), tableName) ||
                    StrUtil.equalsIgnoreCase(StrUtil.trim(table), fullName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 检查表名是否在排除列表中
     */
    private boolean isInExcludedTables(String tableName, String schema) {
        if (StrUtil.isBlank(excludeTables)) {
            return false;
        }

        String fullName = StrUtil.format("{}@{}", tableName, schema);
        List<String> tables = StrUtil.split(excludeTables, ',');

        for (String table : tables) {
            if (StrUtil.isEmpty(table)) {
                continue;
            }

            if (StrUtil.equalsIgnoreCase(StrUtil.trim(table), tableName) ||
                    StrUtil.equalsIgnoreCase(StrUtil.trim(table), fullName)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 检查schema是否在包含列表中
     */
    private boolean isInIncludedSchemas(String schema) {
        if (StrUtil.isBlank(includeSchemas)) {
            return false;
        }

        List<String> schemas = StrUtil.split(includeSchemas, ',');

        for (String includedSchema : schemas) {
            if (StrUtil.isEmpty(includedSchema)) {
                continue;
            }

            if (StrUtil.equalsIgnoreCase(StrUtil.trim(includedSchema), schema)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 将表信息添加到映射中，使用表名+SCHEMA作为唯一标识
     */
    private void addTableInfoToMap(Map<String, TableMetaInfo> tableInfoMap, List<TableInfo> tables, String sourceName) {
        log.info("开始处理{}的{}个表信息...", sourceName, tables.size());

        tables.forEach(table -> {
            // 组合键: tableName#@#schema
            String key = StrUtil.format("{}#@#{}", table.getTableName(), table.getSchema());

            // 获取或创建TableMetaInfo
            TableMetaInfo metaInfo = tableInfoMap.computeIfAbsent(key, k -> new TableMetaInfo(table.getSchema()));

            // 添加数据源和记录数
            metaInfo.addDataSource(sourceName);
            metaInfo.setRecordCount(sourceName, table.getRecordCount());

            // 添加金额字段（从TableInfo直接获取）
            if (metaInfo.getMoneyFields().isEmpty() && !table.getMoneyFields().isEmpty()) {
                try {
                    table.getMoneyFields().forEach(metaInfo::addMoneyField);

                    if (!table.getMoneyFields().isEmpty()) {
                        log.info("表 {}.{} 发现{}个金额字段: {}",
                                table.getSchema(), table.getTableName(),
                                table.getMoneyFields().size(), StrUtil.join(", ", table.getMoneyFields()));
                    }
                } catch (Exception e) {
                    log.error("处理表 {}.{} 的金额字段时出错: {}", table.getSchema(), table.getTableName(), e.getMessage(), e);
                }
            }

            // 添加求和结果
            if (!table.getMoneySums().isEmpty()) {
                table.getMoneySums().forEach((fieldName, sumValue) -> {
                    metaInfo.setMoneySum(sourceName, fieldName, sumValue);
                    log.debug("表 {}.{} 字段 {} 在数据源 {} 的SUM值为: {}",
                            table.getSchema(), table.getTableName(), fieldName, sourceName, sumValue);
                });
            }
        });

        log.info("{}的表信息处理完成", sourceName);
    }

    /**
     * 判断数据类型是否为数值型
     *
     * @param sqlType java.sql.Types中的类型码
     * @return 是否为数值型
     */
    private boolean isNumericType(int sqlType) {
        return sqlType == java.sql.Types.DECIMAL
                || sqlType == java.sql.Types.NUMERIC
                || sqlType == java.sql.Types.DOUBLE
                || sqlType == java.sql.Types.FLOAT
                || sqlType == java.sql.Types.REAL;
    }

    /**
     * 获取需要处理的数据库列表
     *
     * @return 数据库名称和对应JdbcTemplate的映射
     */
    private Map<String, JdbcTemplate> getDatabasesToProcess() {
        Map<String, JdbcTemplate> databases = new LinkedHashMap<>();

        // 全部可用的数据库列表
        Map<String, JdbcTemplate> allDatabases = new LinkedHashMap<>();
        allDatabases.put("ora", oraJdbcTemplate);
        allDatabases.put("rlcms_base", rlcmsBaseJdbcTemplate);
        allDatabases.put("rlcms_pv1", rlcmsPv1JdbcTemplate);
        allDatabases.put("rlcms_pv2", rlcmsPv2JdbcTemplate);
        allDatabases.put("rlcms_pv3", rlcmsPv3JdbcTemplate);
        allDatabases.put("bscopy_pv1", bscopyPv1JdbcTemplate);
        allDatabases.put("bscopy_pv2", bscopyPv2JdbcTemplate);
        allDatabases.put("bscopy_pv3", bscopyPv3JdbcTemplate);

        // 全量重跑模式，返回所有数据库
        if ("FULL".equalsIgnoreCase(runMode)) {
            return allDatabases;
        }

        // 指定库重跑模式
        if (!StrUtil.isEmpty(rerunDatabases)) {
            List<String> dbs = StrUtil.split(rerunDatabases, ',');
            for (String db : dbs) {
                String dbName = StrUtil.trim(db);
                if (allDatabases.containsKey(dbName)) {
                    databases.put(dbName, allDatabases.get(dbName));
                } else {
                    log.warn("未找到指定的数据库[{}]，将被忽略", dbName);
                }
            }
            return databases;
        }

        // 断点续跑模式，只返回未处理的数据库
        for (Map.Entry<String, JdbcTemplate> entry : allDatabases.entrySet()) {
            String dbName = entry.getKey();
            if (!processedDatabases.contains(dbName)) {
                databases.put(dbName, entry.getValue());
            }
        }
        return databases;
    }

    /**
     * 导出金额字段SUM值到Excel
     */
    public void exportMoneyFieldSumToExcel() throws IOException {
        log.info("当前运行模式: {}", runMode);
        if (!StrUtil.isEmpty(rerunDatabases)) {
            log.info("指定重跑数据库: {}", rerunDatabases);
        }
        log.info("开始收集表信息...");

        // 创建一个Map存储表的基本信息
        Map<String, TableMetaInfo> tableInfoMap = new HashMap<>();

        // 获取需要处理的数据库
        Map<String, JdbcTemplate> databasesToProcess = getDatabasesToProcess();
        log.info("本次将处理{}个数据库: {}", databasesToProcess.size(), StrUtil.join(", ", databasesToProcess.keySet()));

        // 使用CompletableFuture并发获取表信息
        List<CompletableFuture<List<TableInfo>>> futures = new ArrayList<>();

        for (Map.Entry<String, JdbcTemplate> entry : databasesToProcess.entrySet()) {
            String dbName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();

            CompletableFuture<List<TableInfo>> future = CompletableFuture.supplyAsync(() -> {
                List<TableInfo> tables = getTablesInfoFromDataSource(jdbcTemplate, dbName);
                log.info("从{}数据源获取到{}个表", dbName, tables.size());
                return tables;
            });

            futures.add(future);
        }

        // 处理所有数据库获取的表信息
        for (CompletableFuture<List<TableInfo>> future : futures) {
            try {
                List<TableInfo> tables = future.get();
                if (tables.isEmpty()) {
                    continue;
                }

                TableInfo firstTable = tables.get(0);
                String dataSourceName = firstTable.getDataSourceName();

                // 添加表信息到映射
                addTableInfoToMap(tableInfoMap, tables, dataSourceName);
            } catch (Exception e) {
                log.error("获取表信息时出错: {}", e.getMessage(), e);
            }
        }

        log.info("表信息收集完成，共发现{}个表", tableInfoMap.size());

        // 创建结果目录，确保能够正确导出文件
        File directory = new File(exportDirectory);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        String outputPath = exportDirectory + File.separator + "表金额字段SUM比对-"
                + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + ".xlsx";

        // 将tableInfoMap转换为展开后的MoneyFieldSumInfo列表 - 声明为最终变量
        final List<MoneyFieldSumInfo> expandedSumInfoList;

        // 对表名进行字母排序
        List<String> sortedKeys = new ArrayList<>(tableInfoMap.keySet());
        Collections.sort(sortedKeys);

        log.info("开始计算各表金额字段SUM值...");

        // 创建一个临时Map用于按表名分组
        Map<String, List<MoneyFieldSumInfo>> tableNameGroupMap = new HashMap<>();

        // 使用TableMetaInfo中的求和结果，直接构建输出信息
        sortedKeys.forEach(key -> {
            List<String> parts = StrUtil.split(key, "#@#");
            String tableName = parts.get(0);
            String schema = parts.get(1);

            TableMetaInfo metaInfo = tableInfoMap.get(key);

            // 对于每个表，如果有金额字段，则为每个金额字段创建一个MoneyFieldSumInfo
            if (!metaInfo.getMoneyFields().isEmpty()) {
                // 对金额字段按字母排序
                List<String> sortedMoneyFields = new ArrayList<>(metaInfo.getMoneyFields());
                Collections.sort(sortedMoneyFields);

                // 为每个金额字段创建MoneyFieldSumInfo并添加到对应表名的分组中
                sortedMoneyFields.forEach(moneyField -> {
                    MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                            tableName,
                            schema,
                            StrUtil.join(" | ", metaInfo.getDataSources()),
                            metaInfo.getFormattedRecordCounts(),
                            metaInfo.getFormattedMoneyFields(),
                            moneyField
                    );

                    // 设置各数据源的记录数和SUM值
                    IntStream.rangeClosed(1, metaInfo.getDataSources().size()).forEach(index -> {
                        String dataSource = metaInfo.getDataSources().get(index - 1);
                        // 设置记录数
                        Long count = metaInfo.getRecordCounts().getOrDefault(dataSource, 0L);
                        sumInfo.setCountValue(index, count);

                        // 设置SUM值
                        BigDecimal sum = metaInfo.getMoneySum(dataSource, moneyField);
                        sumInfo.setSumValue(index, sum);
                    });

                    // 添加到表名分组
                    tableNameGroupMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(sumInfo);
                });
            }
        });

        // 创建并填充结果列表，按表名排序，同一表内按金额字段排序
        expandedSumInfoList = tableNameGroupMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .flatMap(entry -> entry.getValue().stream()
                        .sorted(Comparator.comparing(MoneyFieldSumInfo::getSumField)))
                .collect(Collectors.toList());

        // 导出到Excel
        exportDynamicExcel(outputPath, expandedSumInfoList);

        log.info("金额字段SUM比对结果已成功导出到: {}", outputPath);

        // 在最后，确保所有处理都被标记为已完成
        saveResumeState();
    }

    /**
     * 导出动态行列Excel
     */
    private void exportDynamicExcel(String outputPath, List<MoneyFieldSumInfo> dataList) throws IOException {
        log.info("开始导出Excel: {}", outputPath);

        // 确保数据按表名和金额字段正确排序
        dataList.sort(Comparator.comparing(MoneyFieldSumInfo::getTableName)
                .thenComparing(MoneyFieldSumInfo::getSumField));

        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("金额字段SUM结果");

            // 创建货币样式
            CellStyle numberStyle = workbook.createCellStyle();
            numberStyle.setDataFormat(workbook.createDataFormat().getFormat("#,##0.00")); // 使用千分位分隔符的货币格式

            // 创建表头样式
            CellStyle headerStyle = workbook.createCellStyle();
            headerStyle.setAlignment(HorizontalAlignment.CENTER);
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);

            // 创建"是"样式（绿色背景）
            CellStyle yesStyle = workbook.createCellStyle();
            yesStyle.setAlignment(HorizontalAlignment.CENTER);
            yesStyle.setFillForegroundColor(IndexedColors.LIGHT_GREEN.getIndex());
            yesStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            // 创建"否"样式（红色背景）
            CellStyle noStyle = workbook.createCellStyle();
            noStyle.setAlignment(HorizontalAlignment.CENTER);
            noStyle.setFillForegroundColor(IndexedColors.ROSE.getIndex());
            noStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            // 创建表头行
            Row headerRow = sheet.createRow(0);

            // 固定列的标题
            String[] fixedHeaders = {
                    "表名", "SCHEMA", "所在库",
                    "COUNT_ORA", "COUNT_RLCMS_BASE", "COUNT_RLCMS_PV1", "COUNT_RLCMS_PV2", "COUNT_RLCMS_PV3", "COUNT_BSCOPY_PV1", "COUNT_BSCOPY_PV2", "COUNT_BSCOPY_PV3",
                    "金额字段", "SUM字段",
                    "SUM_ORA", "SUM_RLCMS_BASE", "SUM_RLCMS_PV1", "SUM_RLCMS_PV2", "SUM_RLCMS_PV3", "SUM_BSCOPY_PV1", "SUM_BSCOPY_PV2", "SUM_BSCOPY_PV3",
                    "公式1: ORA==PV1+PV2+PV3",
                    "公式2: ORA==PV1+PV2+PV3",
                    "公式3: ORA==BASE==BSCOPY_PV1==BSCOPY_PV2==BSCOPY_PV3",
                    "公式4: ORA==PV1==PV2==PV3",
                    "公式5: ORA==BASE==PV1==PV2==PV3",
                    "公式6: ORA==PV1"
            };

            // 创建表头
            IntStream.range(0, fixedHeaders.length).forEach(i -> {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(fixedHeaders[i]);
                cell.setCellStyle(headerStyle);
            });

            // 填充数据
            AtomicInteger rowNum = new AtomicInteger(1);
            dataList.forEach(info -> {
                Row row = sheet.createRow(rowNum.getAndIncrement());

                // 设置固定列的值
                row.createCell(0).setCellValue(info.getTableName());
                row.createCell(1).setCellValue(info.getSchema());
                row.createCell(2).setCellValue(info.getDataSources());

                // 设置COUNT值
                IntStream.rangeClosed(1, 8).forEach(i -> {
                    Long countValue = info.getCountValue(i);
                    Cell cell = row.createCell(2 + i);
                    if (countValue != null) {
                        cell.setCellValue(countValue);
                    } else {
                        cell.setCellValue("");
                    }
                });

                row.createCell(11).setCellValue(info.getMoneyFields());
                row.createCell(12).setCellValue(info.getSumField());

                // 设置SUM值（使用金额格式）
                IntStream.rangeClosed(1, 8).forEach(i -> {
                    BigDecimal sumValue = info.getSumValue(i);
                    Cell cell = row.createCell(12 + i);
                    if (sumValue != null) {
                        cell.setCellValue(sumValue.doubleValue());
                        cell.setCellStyle(numberStyle);
                    } else {
                        cell.setCellValue("");
                    }
                });

                // 计算公式列的单元格引用
                // 注意：Excel单元格引用是从1开始的，而且是从A列开始的
                int currentRowIndex = rowNum.get(); // 当前行

                // SUM值所在单元格的列引用
                String sumOraCell = "N" + currentRowIndex;          // SUM_ORA (14列)
                String sumRlcmsBaseCell = "O" + currentRowIndex;    // SUM_RLCMS_BASE (15列)
                String sumRlcmsPv1Cell = "P" + currentRowIndex;     // SUM_RLCMS_PV1 (16列)
                String sumRlcmsPv2Cell = "Q" + currentRowIndex;     // SUM_RLCMS_PV2 (17列)
                String sumRlcmsPv3Cell = "R" + currentRowIndex;     // SUM_RLCMS_PV3 (18列)
                String sumBscopyPv1Cell = "S" + currentRowIndex;    // SUM_BSCOPY_PV1 (19列)
                String sumBscopyPv2Cell = "T" + currentRowIndex;    // SUM_BSCOPY_PV2 (20列)
                String sumBscopyPv3Cell = "U" + currentRowIndex;    // SUM_BSCOPY_PV3 (21列)

                // 公式1: SUM_ORA == SUM_RLCMS_PV1 + SUM_RLCMS_PV2 + SUM_RLCMS_PV3
                Cell formula1Cell = row.createCell(21);
                formula1Cell.setCellFormula("IF(" + sumOraCell + "=(" + sumRlcmsPv1Cell + "+" + sumRlcmsPv2Cell + "+" + sumRlcmsPv3Cell + "),\"是\",\"否\")");

                // 公式2: SUM_ORA == SUM_RLCMS_PV1 + SUM_RLCMS_PV2 + SUM_RLCMS_PV3
                Cell formula2Cell = row.createCell(22);
                formula2Cell.setCellFormula("IF(" + sumOraCell + "=(" + sumRlcmsPv1Cell + "+" + sumRlcmsPv2Cell + "+" + sumRlcmsPv3Cell + "),\"是\",\"否\")");

                // 公式3: SUM_ORA == SUM_RLCMS_BASE == SUM_BSCOPY_PV1 == SUM_BSCOPY_PV2 == SUM_BSCOPY_PV3
                Cell formula3Cell = row.createCell(23);
                formula3Cell.setCellFormula("IF(AND(" +
                        sumOraCell + "=" + sumRlcmsBaseCell + "," +
                        sumRlcmsBaseCell + "=" + sumBscopyPv1Cell + "," +
                        sumBscopyPv1Cell + "=" + sumBscopyPv2Cell + "," +
                        sumBscopyPv2Cell + "=" + sumBscopyPv3Cell + "),\"是\",\"否\")");

                // 公式4: SUM_ORA == SUM_RLCMS_PV1 == SUM_RLCMS_PV2 == SUM_RLCMS_PV3
                Cell formula4Cell = row.createCell(24);
                formula4Cell.setCellFormula("IF(AND(" +
                        sumOraCell + "=" + sumRlcmsPv1Cell + "," +
                        sumRlcmsPv1Cell + "=" + sumRlcmsPv2Cell + "," +
                        sumRlcmsPv2Cell + "=" + sumRlcmsPv3Cell + "),\"是\",\"否\")");

                // 公式5: SUM_ORA == SUM_RLCMS_BASE == SUM_RLCMS_PV1 == SUM_RLCMS_PV2 == SUM_RLCMS_PV3
                Cell formula5Cell = row.createCell(25);
                formula5Cell.setCellFormula("IF(AND(" +
                        sumOraCell + "=" + sumRlcmsBaseCell + "," +
                        sumRlcmsBaseCell + "=" + sumRlcmsPv1Cell + "," +
                        sumRlcmsPv1Cell + "=" + sumRlcmsPv2Cell + "," +
                        sumRlcmsPv2Cell + "=" + sumRlcmsPv3Cell + "),\"是\",\"否\")");

                // 公式6: SUM_ORA == SUM_RLCMS_PV1
                Cell formula6Cell = row.createCell(26);
                formula6Cell.setCellFormula("IF(" + sumOraCell + "=" + sumRlcmsPv1Cell + ",\"是\",\"否\")");

                // 为所有公式单元格添加条件格式
                for (int i = 21; i <= 26; i++) {
                    // 获取当前单元格
                    Cell cell = row.getCell(i);

                    // 创建引用区域字符串
                    String cellRef = cell.getAddress().formatAsString();

                    // 为此单元格添加条件格式规则
                    SheetConditionalFormatting sheetCF = sheet.getSheetConditionalFormatting();

                    // 创建"是"规则
                    ConditionalFormattingRule yesRule = sheetCF.createConditionalFormattingRule("\"是\"=" + cellRef);
                    PatternFormatting yesPattern = yesRule.createPatternFormatting();
                    yesPattern.setFillBackgroundColor(IndexedColors.LIGHT_GREEN.getIndex());
                    yesPattern.setFillPattern(PatternFormatting.SOLID_FOREGROUND);

                    // 创建"否"规则
                    ConditionalFormattingRule noRule = sheetCF.createConditionalFormattingRule("\"否\"=" + cellRef);
                    PatternFormatting noPattern = noRule.createPatternFormatting();
                    noPattern.setFillBackgroundColor(IndexedColors.ROSE.getIndex());
                    noPattern.setFillPattern(PatternFormatting.SOLID_FOREGROUND);

                    // 应用规则到单元格
                    CellRangeAddress[] regions = {new CellRangeAddress(cell.getRowIndex(), cell.getRowIndex(), cell.getColumnIndex(), cell.getColumnIndex())};
                    sheetCF.addConditionalFormatting(regions, yesRule, noRule);
                }
            });

            // 自动调整列宽
            IntStream.range(0, fixedHeaders.length).forEach(sheet::autoSizeColumn);

            // 写入文件
            try (FileOutputStream fileOut = new FileOutputStream(outputPath)) {
                workbook.write(fileOut);
            }
        }
    }

    /**
     * 表信息类
     */
    @Data
    private static class TableInfo {
        private final String tableName;
        private final String schema;
        private final List<String> moneyFields = new ArrayList<>();
        private long recordCount = 0L;
        private final Map<String, BigDecimal> moneySums = new HashMap<>();
        private String dataSourceName;

        public TableInfo(String tableName, String schema) {
            this.tableName = tableName;
            this.schema = schema;
        }
    }

    /**
     * 表元信息类
     */
    @Data
    private static class TableMetaInfo {
        private final String schema;
        private final List<String> dataSources = new ArrayList<>();
        private final Map<String, Long> recordCounts = new HashMap<>();
        private final Set<String> moneyFields = new HashSet<>();
        private final Map<String, Map<String, BigDecimal>> moneySums = new HashMap<>();

        public TableMetaInfo(String schema) {
            this.schema = schema;
        }

        public void addDataSource(String dataSource) {
            if (!dataSources.contains(dataSource)) {
                dataSources.add(dataSource);
            }
        }

        public void setRecordCount(String dataSource, long count) {
            recordCounts.put(dataSource, count);
        }

        public String getFormattedRecordCounts() {
            // 确保记录数与数据源顺序一致
            List<String> counts = new ArrayList<>();
            for (String dataSource : dataSources) {
                Long count = recordCounts.getOrDefault(dataSource, 0L);
                counts.add(StrUtil.toString(count));
            }
            return StrUtil.join("|", counts);
        }

        public void addMoneyField(String fieldName) {
            moneyFields.add(fieldName);
        }

        public String getFormattedMoneyFields() {
            return StrUtil.join("|", moneyFields);
        }

        // 添加SUM值的方法
        public void setMoneySum(String dataSource, String fieldName, BigDecimal sum) {
            Map<String, BigDecimal> sourceSums = moneySums.computeIfAbsent(dataSource, k -> new HashMap<>());
            sourceSums.put(fieldName, sum);
        }

        // 获取指定数据源和字段的SUM值
        public BigDecimal getMoneySum(String dataSource, String fieldName) {
            Map<String, BigDecimal> sourceSums = moneySums.get(dataSource);
            if (sourceSums == null) {
                return BigDecimal.ZERO;
            }
            return sourceSums.getOrDefault(fieldName, BigDecimal.ZERO);
        }
    }

}