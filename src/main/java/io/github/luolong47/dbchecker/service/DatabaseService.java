package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.config.DbWhereConditionConfig;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.BadSqlGrammarException;
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
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

    // 存储所有表的元数据信息，用于断点续跑时恢复状态
    private final Map<String, TableMetaInfo> tableInfoMap = new HashMap<>();

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
        log.info("初始化数据库服务...");
        log.info("配置信息 - 导出目录: {}", exportDirectory);
        log.info("配置信息 - 运行模式: {}", runMode);
        log.info("配置信息 - 断点续跑文件: {}", resumeFile);
        log.info("配置信息 - 包含表: {}", includeTables);
        log.info("配置信息 - 包含Schema: {}", includeSchemas);

        // 检查必要的配置项
        if (StrUtil.isBlank(includeTables)) {
            throw new IllegalArgumentException("db.include.tables未设置，这是一个必填项。请在application.yml中指定需要处理的表列表。");
        }

        // 清空当前状态，准备重新加载
        processedDatabases.clear();
        processedTables.clear();
        tableInfoMap.clear();

        // 只在断点续跑模式下加载状态
        if ("RESUME".equalsIgnoreCase(runMode)) {
            try {
                File file = FileUtil.file(resumeFile);
                if (file.exists()) {
                    log.info("检测到断点续跑文件: {}", resumeFile);

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

                    // 恢复表信息数据
                    JSONObject tableInfoObj = state.getJSONObject("tableInfo");
                    if (tableInfoObj != null) {
                        for (String tableName : tableInfoObj.keySet()) {
                            JSONObject tableData = tableInfoObj.getJSONObject(tableName);
                            if (tableData != null) {
                                // 创建新的TableMetaInfo对象
                                TableMetaInfo metaInfo = new TableMetaInfo();

                                // 恢复数据源列表
                                JSONArray dataSources = tableData.getJSONArray("dataSources");
                                if (dataSources != null) {
                                    for (int i = 0; i < dataSources.size(); i++) {
                                        metaInfo.addDataSource(dataSources.getStr(i));
                                    }
                                }

                                // 恢复记录数
                                JSONObject recordCounts = tableData.getJSONObject("recordCounts");
                                if (recordCounts != null) {
                                    for (String ds : recordCounts.keySet()) {
                                        metaInfo.setRecordCount(ds, recordCounts.getLong(ds));
                                    }
                                }

                                // 恢复金额字段
                                JSONArray moneyFields = tableData.getJSONArray("moneyFields");
                                if (moneyFields != null) {
                                    for (int i = 0; i < moneyFields.size(); i++) {
                                        metaInfo.addMoneyField(moneyFields.getStr(i));
                                    }
                                }

                                // 恢复金额SUM值
                                JSONObject moneySums = tableData.getJSONObject("moneySums");
                                if (moneySums != null) {
                                    for (String ds : moneySums.keySet()) {
                                        JSONObject fieldSums = moneySums.getJSONObject(ds);
                                        if (fieldSums != null) {
                                            for (String field : fieldSums.keySet()) {
                                                String sumStr = fieldSums.getStr(field);
                                                if (sumStr != null) {
                                                    try {
                                                        BigDecimal sum = new BigDecimal(sumStr);
                                                        metaInfo.setMoneySum(ds, field, sum);
                                                    } catch (NumberFormatException e) {
                                                        log.warn("恢复金额字段SUM值时出错: {}字段{}的值{}不是有效的数值",
                                                            ds, field, sumStr);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }

                                // 将恢复的元数据添加到表信息映射
                                tableInfoMap.put(tableName, metaInfo);
                            }
                        }
                    }

                    log.info("成功从断点续跑文件恢复状态，已处理数据库: {}，已处理表: {}，表信息数: {}",
                        processedDatabases.size(), processedTables.size(), tableInfoMap.size());
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
    private void saveResumeState() {
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
                for (Map.Entry<String, TableMetaInfo> entry : tableInfoMap.entrySet()) {
                    Map<String, Object> tableData = new HashMap<>();
                    TableMetaInfo metaInfo = entry.getValue();

                    // 保存数据源列表
                    tableData.put("dataSources", metaInfo.getDataSources());

                    // 保存记录数
                    tableData.put("recordCounts", metaInfo.getRecordCounts());

                    // 保存金额字段
                    tableData.put("moneyFields", new ArrayList<>(metaInfo.getMoneyFields()));

                    // 保存金额SUM值
                    Map<String, Map<String, String>> moneySumsStr = new HashMap<>();
                    for (Map.Entry<String, Map<String, BigDecimal>> dsEntry : metaInfo.getMoneySums().entrySet()) {
                        Map<String, String> fieldSums = new HashMap<>();
                        for (Map.Entry<String, BigDecimal> sumEntry : dsEntry.getValue().entrySet()) {
                            if (sumEntry.getValue() != null) {
                                fieldSums.put(sumEntry.getKey(), sumEntry.getValue().toString());
                            }
                        }
                        moneySumsStr.put(dsEntry.getKey(), fieldSums);
                    }
                    tableData.put("moneySums", moneySumsStr);

                    serializedTableInfo.put(entry.getKey(), tableData);
                }

                json.set("tableInfo", serializedTableInfo);
            }

            // 写入文件
            FileUtil.writeUtf8String(json.toString(), resumeFile);
            log.info("已保存断点续跑状态到{}，包含{}个数据库和{}个表的信息",
                resumeFile, processedDatabases.size(), processedTables.size());
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
     * @param dataSourceName 数据源名称
     * @return 是否应该处理
     */
    private boolean shouldProcessTable(String tableName, String dataSourceName) {
        // 标识符：数据源名称 + 表名
        String tableKey = StrUtil.format("{}|{}", dataSourceName, tableName);

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
     */
    private void markTableProcessed(String tableName, String dataSourceName) {
        // 标识符：数据源名称 + 表名
        String tableKey = StrUtil.format("{}|{}", dataSourceName, tableName);
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
                    !shouldProcessTable(tableName, dataSourceName)) {
                    continue;
                }

                TableInfo tableInfo = new TableInfo(tableName);
                tableInfo.setDataSourceName(dataSourceName);

                // 获取金额字段并添加到TableInfo中
                try {
                    // 获取列信息
                    try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
                        while (columns.next()) {
                            String columnName = columns.getString("COLUMN_NAME");
                            int dataType = columns.getInt("DATA_TYPE");
                            int decimalDigits = columns.getInt("DECIMAL_DIGITS");

                            // 判断是否为金额字段：数值型且小数位不为0
                            if (isNumericType(dataType) && decimalDigits > 0) {
                                tableInfo.getMoneyFields().add(columnName);
                                log.debug("发现金额字段: {}.{}, 类型: {}, 小数位: {}",
                                    tableName, columnName, dataType, decimalDigits);
                            }
                        }
                    }

                    if (!tableInfo.getMoneyFields().isEmpty()) {
                        log.info("表[{}]中发现{}个金额字段: {}", tableName, tableInfo.getMoneyFields().size(), StrUtil.join(", ", tableInfo.getMoneyFields()));
                    }

                    // 获取表记录数
                    try {
                        // 查询记录数
                        String countSql = StrUtil.format("SELECT COUNT(*) FROM {}", tableName);
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
                                tableName);

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
                                log.debug("表 {} 字段 {} 的SUM值为: {}", tableName, fieldName, decimalValue);
                            }
                        }
                        tables.add(tableInfo);
                        // 标记该表为已处理
                        markTableProcessed(tableName, dataSourceName);
                    } catch (BadSqlGrammarException e) {
                        log.error("获取表[{}]的记录数或SUM值时出错: {}", tableName, e.getMessage());
                    } catch (Exception e) {
                        log.error("获取表[{}]的记录数或SUM值时出错: {}", tableName, e.getMessage(), e);
                    }
                } catch (SQLException e) {
                    log.error("获取表[{}]的金额字段时出错: {}", tableName, e.getMessage(), e);
                }
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
     */
    private boolean shouldExcludeTable(String tableName, String schema) {

        boolean matchSchema = StrUtil.split(includeSchemas, ",").stream()
            .map(String::trim)
            .anyMatch(e -> StrUtil.equals(schema, e));
        if (!matchSchema) {
            return true;
        }

        return StrUtil.split(includeTables, ",").stream()
            .map(String::trim)
            .map(e -> StrUtil.split(e, "@").stream().findFirst().orElse(""))
            .noneMatch(e -> StrUtil.equals(tableName, e));
    }

    /**
     * 将表信息添加到映射中，使用表名作为唯一标识
     */
    private void addTableInfoToMap(Map<String, TableMetaInfo> tableInfoMap, List<TableInfo> tables, String sourceName) {
        log.info("开始处理{}的{}个表信息...", sourceName, tables.size());

        tables.forEach(table -> {
            // 使用表名作为键，不再包含schema
            String key = table.getTableName();

            // 获取或创建TableMetaInfo，不再传入schema
            TableMetaInfo metaInfo = tableInfoMap.computeIfAbsent(key, k -> new TableMetaInfo());

            // 添加数据源和记录数
            metaInfo.addDataSource(sourceName);
            metaInfo.setRecordCount(sourceName, table.getRecordCount());

            // 添加金额字段（从TableInfo直接获取）
            if (metaInfo.getMoneyFields().isEmpty() && !table.getMoneyFields().isEmpty()) {
                try {
                    table.getMoneyFields().forEach(metaInfo::addMoneyField);

                    if (!table.getMoneyFields().isEmpty()) {
                        log.info("表 {} 发现{}个金额字段: {}",
                            table.getTableName(),
                            table.getMoneyFields().size(), StrUtil.join(", ", table.getMoneyFields()));
                    }
                } catch (Exception e) {
                    log.error("处理表 {} 的金额字段时出错: {}", table.getTableName(), e.getMessage(), e);
                }
            }

            // 添加求和结果
            if (!table.getMoneySums().isEmpty()) {
                table.getMoneySums().forEach((fieldName, sumValue) -> {
                    metaInfo.setMoneySum(sourceName, fieldName, sumValue);
                    log.debug("表 {} 字段 {} 在数据源 {} 的SUM值为: {}",
                        table.getTableName(), fieldName, sourceName, sumValue);
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
     * 导出金额字段的SUM值到Excel文件
     */
    public void exportMoneyFieldSumToExcel() throws IOException {
        log.info("当前运行模式: {}", runMode);
        if (!StrUtil.isEmpty(rerunDatabases)) {
            log.info("指定重跑数据库: {}", rerunDatabases);
        }

        // 检查是否已从断点续跑文件恢复了表信息
        boolean hasRestoredTableInfo = !tableInfoMap.isEmpty();

        if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(runMode)) {
            log.info("已从断点续跑文件恢复{}个表的信息，将继续处理未完成的数据库和表", tableInfoMap.size());
        } else {
            log.info("开始收集表信息...");
            // 如果没有恢复数据或不是断点续跑模式，则清空已有的表信息
            if (!hasRestoredTableInfo || !"RESUME".equalsIgnoreCase(runMode)) {
                tableInfoMap.clear();
            }
        }

        // 获取需要处理的数据库
        Map<String, JdbcTemplate> databasesToProcess = getDatabasesToProcess();
        log.info("本次将处理{}个数据库: {}", databasesToProcess.size(), StrUtil.join(", ", databasesToProcess.keySet()));

        // 使用CompletableFuture并发获取表信息
        List<CompletableFuture<List<TableInfo>>> futures = new ArrayList<>();

        for (Map.Entry<String, JdbcTemplate> entry : databasesToProcess.entrySet()) {
            String dbName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();

            // 如果数据库已经处理过且从断点续跑恢复了数据，则跳过
            if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(runMode) && processedDatabases.contains(dbName)) {
                log.info("数据库[{}]已在上次运行中处理，跳过", dbName);
                continue;
            }

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

                // 及时保存状态
                saveResumeState();
            } catch (Exception e) {
                log.error("获取表信息时出错: {}", e.getMessage(), e);
            }
        }

        log.info("表信息收集完成，共发现{}个表", tableInfoMap.size());

        // 创建结果目录，确保能够正确导出文件
        File directory = FileUtil.file(exportDirectory);
        if (!directory.exists()) {
            FileUtil.mkdir(directory);
        }

        File outputFile = FileUtil.file(directory, StrUtil.format("表金额字段SUM比对-{}.xlsx", DateUtil.format(DateUtil.date(), "yyyyMMdd-HHmmss")));

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
            // 移除对schema的依赖，直接使用表名作为键

            TableMetaInfo metaInfo = tableInfoMap.get(key);

            // 创建一个默认的MoneyFieldSumInfo，即使没有金额字段
            MoneyFieldSumInfo defaultSumInfo = new MoneyFieldSumInfo(
                key,
                metaInfo,
                ""
            );

            if (metaInfo.getMoneyFields().isEmpty()) {
                // 如果没有金额字段，添加默认条目
                tableNameGroupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(defaultSumInfo);
            } else {
                // 对金额字段按字母排序
                List<String> sortedMoneyFields = new ArrayList<>(metaInfo.getMoneyFields());
                Collections.sort(sortedMoneyFields);

                // 为每个金额字段创建MoneyFieldSumInfo并添加到对应表名的分组中
                sortedMoneyFields.forEach(moneyField -> {
                    MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                        key,
                        metaInfo,
                        moneyField
                    );

                    // 添加到表名分组
                    tableNameGroupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(sumInfo);
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
        exportDynamicExcel(outputFile, expandedSumInfoList);

        log.info("金额字段SUM比对结果已成功导出到: {}", outputFile);

        // 在最后，确保所有处理都被标记为已完成
        saveResumeState();
    }

    /**
     * 导出动态行列Excel
     */
    private void exportDynamicExcel(File outputFile, List<MoneyFieldSumInfo> dataList) {
        log.info("开始导出Excel: {}", outputFile.getAbsolutePath());
        log.info("总数据量: {} 条记录", dataList.size());

        // 确保数据按表名和金额字段正确排序
        dataList.sort(Comparator.comparing(MoneyFieldSumInfo::getTableName)
            .thenComparing(MoneyFieldSumInfo::getSumField));

        // 创建支持流式写入的工作簿，设置内存中保留100行，其余写入临时文件
        SXSSFWorkbook workbook = new SXSSFWorkbook(null, 100, true, true);
        try {
            SXSSFSheet sheet = workbook.createSheet("金额字段SUM结果");

            // 设置表格名称
            Row headerRow = sheet.createRow(0);

            // 固定列的标题（移除SCHEMA列）
            String[] fixedHeaders = {
                "表名", "所在库",
                "COUNT_ORA", "COUNT_RLCMS_BASE", "COUNT_RLCMS_PV1", "COUNT_RLCMS_PV2", "COUNT_RLCMS_PV3", "COUNT_BSCOPY_PV1", "COUNT_BSCOPY_PV2", "COUNT_BSCOPY_PV3",
                "金额字段", "SUM字段",
                "SUM_ORA", "SUM_RLCMS_BASE", "SUM_RLCMS_PV1", "SUM_RLCMS_PV2", "SUM_RLCMS_PV3", "SUM_BSCOPY_PV1", "SUM_BSCOPY_PV2", "SUM_BSCOPY_PV3",
                "公式1: ORA==PV1+PV2+PV3",
                "公式2: ORA==PV1+PV2+PV3",
                "公式3: ORA==BASE==BSCOPY_PV1==BSCOPY_PV2==BSCOPY_PV3",
                "公式4: ORA==PV1==PV2==PV3",
                "公式5: ORA==BASE==PV1==PV2==PV3",
                "公式6: ORA==PV1",
                "COUNT公式1: ORA==PV1+PV2+PV3",
                "COUNT公式2: ORA==PV1+PV2+PV3",
                "COUNT公式3: ORA==BASE==BSCOPY_PV1==BSCOPY_PV2==BSCOPY_PV3",
                "COUNT公式4: ORA==PV1==PV2==PV3",
                "COUNT公式5: ORA==BASE==PV1==PV2==PV3",
                "COUNT公式6: ORA==PV1"
            };

            // 设置表头
            for (int i = 0; i < fixedHeaders.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(fixedHeaders[i]);
            }

            // 创建样式
            CellStyle headerStyle = workbook.createCellStyle();
            headerStyle.setAlignment(HorizontalAlignment.CENTER);
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);

            // 应用表头样式
            for (int i = 0; i < fixedHeaders.length; i++) {
                headerRow.getCell(i).setCellStyle(headerStyle);
            }

            // 设置货币样式
            CellStyle numberStyle = workbook.createCellStyle();
            DataFormat format = workbook.createDataFormat();
            numberStyle.setDataFormat(format.getFormat("#,##0.00"));

            // 填充数据
            int rowNum = 1;
            final int BATCH_SIZE = 100; // 每100行刷新一次
            int processedCount = 0;
            int totalSize = dataList.size();

            for (MoneyFieldSumInfo info : dataList) {
                Row row = sheet.createRow(rowNum++);

                // 设置固定列的值
                row.createCell(0).setCellValue(info.getTableName());
                row.createCell(1).setCellValue(info.getDataSources());

                // 设置COUNT值
                setCellValue(row.createCell(2), info.getCountValueByDataSource("ora"));
                setCellValue(row.createCell(3), info.getCountValueByDataSource("rlcms_base"));
                setCellValue(row.createCell(4), info.getCountValueByDataSource("rlcms_pv1"));
                setCellValue(row.createCell(5), info.getCountValueByDataSource("rlcms_pv2"));
                setCellValue(row.createCell(6), info.getCountValueByDataSource("rlcms_pv3"));
                setCellValue(row.createCell(7), info.getCountValueByDataSource("bscopy_pv1"));
                setCellValue(row.createCell(8), info.getCountValueByDataSource("bscopy_pv2"));
                setCellValue(row.createCell(9), info.getCountValueByDataSource("bscopy_pv3"));

                // 设置金额字段和SUM字段
                row.createCell(10).setCellValue(info.getMoneyFields());
                row.createCell(11).setCellValue(info.getSumField());

                // 设置SUM值
                setMoneyValue(row.createCell(12), info.getSumValueByDataSource("ora"), numberStyle);
                setMoneyValue(row.createCell(13), info.getSumValueByDataSource("rlcms_base"), numberStyle);
                setMoneyValue(row.createCell(14), info.getSumValueByDataSource("rlcms_pv1"), numberStyle);
                setMoneyValue(row.createCell(15), info.getSumValueByDataSource("rlcms_pv2"), numberStyle);
                setMoneyValue(row.createCell(16), info.getSumValueByDataSource("rlcms_pv3"), numberStyle);
                setMoneyValue(row.createCell(17), info.getSumValueByDataSource("bscopy_pv1"), numberStyle);
                setMoneyValue(row.createCell(18), info.getSumValueByDataSource("bscopy_pv2"), numberStyle);
                setMoneyValue(row.createCell(19), info.getSumValueByDataSource("bscopy_pv3"), numberStyle);

                // 设置公式
                setFormulas(row, sheet);

                processedCount++;
                if (processedCount % BATCH_SIZE == 0) {
                    sheet.flushRows(); // 强制刷新到磁盘
                    log.info("已处理 {}/{} 行 ({}%)",
                        processedCount, totalSize,
                        Math.round((processedCount * 100.0) / totalSize));
                }
            }

            // 设置列宽
            for (int i = 0; i < fixedHeaders.length; i++) {
                sheet.setColumnWidth(i, 4000); // 设置一个合理的默认宽度
            }
            sheet.setColumnWidth(0, 6000); // 表名列加宽
            sheet.setColumnWidth(1, 8000); // 所在库列加宽
            sheet.setColumnWidth(10, 6000); // 金额字段列加宽

            // 写入文件
            try (FileOutputStream fileOut = new FileOutputStream(outputFile)) {
                workbook.write(fileOut);
            }

            log.info("Excel导出完成: {}", outputFile.getAbsolutePath());
        } catch (Exception e) {
            log.error("导出Excel时发生错误: {}", e.getMessage(), e);
            throw new RuntimeException("导出Excel失败", e);
        } finally {
            try {
                // 关闭工作簿
                workbook.close();
            } catch (IOException e) {
                log.error("关闭工作簿时发生错误: {}", e.getMessage(), e);
            }
        }
    }

    /**
     * 设置单元格数值
     */
    private void setCellValue(Cell cell, Long value) {
        if (value != null) {
            cell.setCellValue(value);
        } else {
            cell.setCellValue("");
        }
    }

    /**
     * 设置金额单元格值
     */
    private void setMoneyValue(Cell cell, BigDecimal value, CellStyle style) {
        if (value != null) {
            cell.setCellValue(value.doubleValue());
            cell.setCellStyle(style);
        } else {
            cell.setCellValue("");
        }
    }

    /**
     * 设置公式
     */
    private void setFormulas(Row row, Sheet sheet) {
        int currentRow = row.getRowNum() + 1;

        // 设置SUM值公式
        String sumOraCell = "M" + currentRow;
        String sumRlcmsBaseCell = "N" + currentRow;
        String sumRlcmsPv1Cell = "O" + currentRow;
        String sumRlcmsPv2Cell = "P" + currentRow;
        String sumRlcmsPv3Cell = "Q" + currentRow;
        String sumBscopyPv1Cell = "R" + currentRow;
        String sumBscopyPv2Cell = "S" + currentRow;
        String sumBscopyPv3Cell = "T" + currentRow;

        // COUNT值单元格引用
        String countOraCell = "C" + currentRow;
        String countRlcmsBaseCell = "D" + currentRow;
        String countRlcmsPv1Cell = "E" + currentRow;
        String countRlcmsPv2Cell = "F" + currentRow;
        String countRlcmsPv3Cell = "G" + currentRow;
        String countBscopyPv1Cell = "H" + currentRow;
        String countBscopyPv2Cell = "I" + currentRow;
        String countBscopyPv3Cell = "J" + currentRow;

        // 公式1: ORA==PV1+PV2+PV3
        setFormula(row, 20, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), IF(ABS(%s-(%s+%s+%s))<=0.01, \"TRUE\", \"FALSE\"), \"N/A\")",
            sumOraCell, sumRlcmsPv1Cell, sumRlcmsPv2Cell, sumRlcmsPv3Cell,
            sumOraCell, sumRlcmsPv1Cell, sumRlcmsPv2Cell, sumRlcmsPv3Cell));

        // 公式2: ORA==PV1+PV2+PV3
        setFormula(row, 21, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), IF(ABS(%s-(%s+%s+%s))<=0.01, \"TRUE\", \"FALSE\"), \"N/A\")",
            sumOraCell, sumRlcmsPv1Cell, sumRlcmsPv2Cell, sumRlcmsPv3Cell,
            sumOraCell, sumRlcmsPv1Cell, sumRlcmsPv2Cell, sumRlcmsPv3Cell));

        // 公式3: ORA==BASE==BSCOPY_PV1==BSCOPY_PV2==BSCOPY_PV3
        setFormula(row, 22, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), " +
                "IF(AND(ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01), \"TRUE\", \"FALSE\"), \"N/A\")",
            sumOraCell, sumRlcmsBaseCell, sumBscopyPv1Cell, sumBscopyPv2Cell, sumBscopyPv3Cell,
            sumOraCell, sumRlcmsBaseCell, sumOraCell, sumBscopyPv1Cell, sumOraCell, sumBscopyPv2Cell, sumOraCell, sumBscopyPv3Cell));

        // 公式4: ORA==PV1==PV2==PV3
        setFormula(row, 23, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), " +
                "IF(AND(ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01), \"TRUE\", \"FALSE\"), \"N/A\")",
            sumOraCell, sumRlcmsPv1Cell, sumRlcmsPv2Cell, sumRlcmsPv3Cell,
            sumOraCell, sumRlcmsPv1Cell, sumOraCell, sumRlcmsPv2Cell, sumOraCell, sumRlcmsPv3Cell));

        // 公式5: ORA==BASE==PV1==PV2==PV3
        setFormula(row, 24, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), " +
                "IF(AND(ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01, ABS(%s-%s)<=0.01), \"TRUE\", \"FALSE\"), \"N/A\")",
            sumOraCell, sumRlcmsBaseCell, sumRlcmsPv1Cell, sumRlcmsPv2Cell, sumRlcmsPv3Cell,
            sumOraCell, sumRlcmsBaseCell, sumOraCell, sumRlcmsPv1Cell, sumOraCell, sumRlcmsPv2Cell, sumOraCell, sumRlcmsPv3Cell));

        // 公式6: ORA==PV1
        setFormula(row, 25, String.format(
            "IF(AND(%s<>\"\", %s<>\"\"), IF(ABS(%s-%s)<=0.01, \"TRUE\", \"FALSE\"), \"N/A\")",
            sumOraCell, sumRlcmsPv1Cell, sumOraCell, sumRlcmsPv1Cell));

        // COUNT公式1: ORA==PV1+PV2+PV3
        setFormula(row, 26, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), IF(%s=(%s+%s+%s), \"TRUE\", \"FALSE\"), \"N/A\")",
            countOraCell, countRlcmsPv1Cell, countRlcmsPv2Cell, countRlcmsPv3Cell,
            countOraCell, countRlcmsPv1Cell, countRlcmsPv2Cell, countRlcmsPv3Cell));

        // COUNT公式2: ORA==PV1+PV2+PV3
        setFormula(row, 27, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), IF(%s=(%s+%s+%s), \"TRUE\", \"FALSE\"), \"N/A\")",
            countOraCell, countRlcmsPv1Cell, countRlcmsPv2Cell, countRlcmsPv3Cell,
            countOraCell, countRlcmsPv1Cell, countRlcmsPv2Cell, countRlcmsPv3Cell));

        // COUNT公式3: ORA==BASE==BSCOPY_PV1==BSCOPY_PV2==BSCOPY_PV3
        setFormula(row, 28, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), " +
                "IF(AND(%s=%s, %s=%s, %s=%s, %s=%s), \"TRUE\", \"FALSE\"), \"N/A\")",
            countOraCell, countRlcmsBaseCell, countBscopyPv1Cell, countBscopyPv2Cell, countBscopyPv3Cell,
            countOraCell, countRlcmsBaseCell, countOraCell, countBscopyPv1Cell, countOraCell, countBscopyPv2Cell, countOraCell, countBscopyPv3Cell));

        // COUNT公式4: ORA==PV1==PV2==PV3
        setFormula(row, 29, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), " +
                "IF(AND(%s=%s, %s=%s, %s=%s), \"TRUE\", \"FALSE\"), \"N/A\")",
            countOraCell, countRlcmsPv1Cell, countRlcmsPv2Cell, countRlcmsPv3Cell,
            countOraCell, countRlcmsPv1Cell, countOraCell, countRlcmsPv2Cell, countOraCell, countRlcmsPv3Cell));

        // COUNT公式5: ORA==BASE==PV1==PV2==PV3
        setFormula(row, 30, String.format(
            "IF(AND(%s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\", %s<>\"\"), " +
                "IF(AND(%s=%s, %s=%s, %s=%s, %s=%s), \"TRUE\", \"FALSE\"), \"N/A\")",
            countOraCell, countRlcmsBaseCell, countRlcmsPv1Cell, countRlcmsPv2Cell, countRlcmsPv3Cell,
            countOraCell, countRlcmsBaseCell, countOraCell, countRlcmsPv1Cell, countOraCell, countRlcmsPv2Cell, countOraCell, countRlcmsPv3Cell));

        // COUNT公式6: ORA==PV1
        setFormula(row, 31, String.format(
            "IF(AND(%s<>\"\", %s<>\"\"), IF(%s=%s, \"TRUE\", \"FALSE\"), \"N/A\")",
            countOraCell, countRlcmsPv1Cell, countOraCell, countRlcmsPv1Cell));

        // 设置条件格式
        setConditionalFormatting(sheet, row);
    }

    /**
     * 设置单个公式
     */
    private void setFormula(Row row, int columnIndex, String formula) {
        Cell cell = row.createCell(columnIndex);
        cell.setCellFormula(formula);
    }

    /**
     * 设置条件格式
     */
    private void setConditionalFormatting(Sheet sheet, Row row) {
        SheetConditionalFormatting sheetCF = sheet.getSheetConditionalFormatting();

        // 为公式结果列设置条件格式（20-31列）
        for (int i = 20; i <= 31; i++) {
            Cell cell = row.getCell(i);
            String cellRef = cell.getAddress().formatAsString();

            // TRUE的样式规则
            ConditionalFormattingRule trueRule = sheetCF.createConditionalFormattingRule("\"TRUE\"=" + cellRef);
            PatternFormatting truePattern = trueRule.createPatternFormatting();
            truePattern.setFillBackgroundColor(IndexedColors.LIGHT_GREEN.getIndex());
            truePattern.setFillPattern(PatternFormatting.SOLID_FOREGROUND);

            // FALSE的样式规则
            ConditionalFormattingRule falseRule = sheetCF.createConditionalFormattingRule("\"FALSE\"=" + cellRef);
            PatternFormatting falsePattern = falseRule.createPatternFormatting();
            falsePattern.setFillBackgroundColor(IndexedColors.ROSE.getIndex());
            falsePattern.setFillPattern(PatternFormatting.SOLID_FOREGROUND);

            // 应用规则
            CellRangeAddress[] regions = {
                new CellRangeAddress(cell.getRowIndex(), cell.getRowIndex(),
                    cell.getColumnIndex(), cell.getColumnIndex())
            };
            sheetCF.addConditionalFormatting(regions, trueRule, falseRule);
        }
    }

    /**
     * 表信息类
     */
    @Data
    private static class TableInfo {
        private final String tableName;
        private long recordCount = 0L;
        private final List<String> moneyFields = new ArrayList<>();
        private final Map<String, BigDecimal> moneySums = new HashMap<>();
        private String dataSourceName;

        public TableInfo(String tableName) {
            this.tableName = tableName;
        }
    }

    /**
     * 表元信息类
     */
    @Data
    private static class TableMetaInfo {
        private final List<String> dataSources = new ArrayList<>();
        private final Map<String, Long> recordCounts = new HashMap<>();
        private final Set<String> moneyFields = new HashSet<>();
        /**
         * -- GETTER --
         * 获取所有金额字段的SUM值映射
         */
        @Getter
        private final Map<String, Map<String, BigDecimal>> moneySums = new HashMap<>();

        public void addDataSource(String dataSource) {
            if (!this.dataSources.contains(dataSource)) {
                this.dataSources.add(dataSource);
            }
        }

        public void setRecordCount(String dataSource, long count) {
            this.recordCounts.put(dataSource, count);
        }

        public void addMoneyField(String fieldName) {
            this.moneyFields.add(fieldName);
        }

        public String getFormattedMoneyFields() {
            return StrUtil.join(" | ", moneyFields);
        }

        public void setMoneySum(String dataSource, String fieldName, BigDecimal sum) {
            this.moneySums.computeIfAbsent(dataSource, k -> new HashMap<>()).put(fieldName, sum);
        }

        public BigDecimal getMoneySum(String dataSource, String fieldName) {
            Map<String, BigDecimal> dataSourceSums = this.moneySums.get(dataSource);
            return dataSourceSums != null ? dataSourceSums.getOrDefault(fieldName, null) : null;
        }

    }

    @Data
    public static class MoneyFieldSumInfo {
        private final String tableName;
        private final String dataSources;
        private final Map<String, Long> countValues = new HashMap<>();
        private final String moneyFields;
        private final String sumField;
        private final Map<String, BigDecimal> sumValues = new HashMap<>();

        // 在构造函数中保存数据源和对应的COUNT值和SUM值
        public MoneyFieldSumInfo(String tableName, TableMetaInfo metaInfo, String sumField) {
            this.tableName = tableName;
            this.dataSources = String.join(" | ", metaInfo.getDataSources());
            this.moneyFields = metaInfo.getFormattedMoneyFields();
            this.sumField = sumField;

            // 直接保存每个数据源的COUNT值
            for (String ds : metaInfo.getDataSources()) {
                Long count = metaInfo.getRecordCounts().get(ds);
                if (count != null) {
                    countValues.put(ds.toLowerCase(), count);
                }
            }

            // 直接保存每个数据源的SUM值
            for (String ds : metaInfo.getDataSources()) {
                BigDecimal sum = metaInfo.getMoneySum(ds, sumField);
                if (sum != null) {
                    sumValues.put(ds.toLowerCase(), sum);
                }
            }
        }

        // 根据数据源名称获取COUNT值
        public Long getCountValueByDataSource(String dataSource) {
            return countValues.get(dataSource.toLowerCase());
        }

        // 根据数据源名称获取SUM值
        public BigDecimal getSumValueByDataSource(String dataSource) {
            return sumValues.get(dataSource.toLowerCase());
        }

    }

}