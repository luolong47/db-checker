package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.config.DbWhereConditionConfig;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
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
    private Set<String> processedDatabases = new HashSet<>();
    private Set<String> processedTables = new HashSet<>();

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
        if (!StringUtils.isEmpty(rerunDatabases)) {
            String[] dbs = rerunDatabases.split(",");
            for (String db : dbs) {
                if (db.trim().equalsIgnoreCase(dataSourceName)) {
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
     * @param tableName 表名
     * @param schema 表所在的schema
     * @param dataSourceName 数据源名称
     * @return 是否应该处理
     */
    private boolean shouldProcessTable(String tableName, String schema, String dataSourceName) {
        // 标识符：数据源名称 + schema + 表名
        String tableKey = dataSourceName + "|" + (schema != null ? schema : "") + "|" + tableName;
        
        // 如果是全量重跑，总是处理
        if ("FULL".equalsIgnoreCase(runMode)) {
            return true;
        }
        
        // 如果是指定库重跑，检查是否在指定列表中
        if (!StringUtils.isEmpty(rerunDatabases)) {
            String[] dbs = rerunDatabases.split(",");
            for (String db : dbs) {
                if (db.trim().equalsIgnoreCase(dataSourceName)) {
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
     * @param tableName 表名
     * @param schema 表所在的schema
     * @param dataSourceName 数据源名称
     */
    private void markTableProcessed(String tableName, String schema, String dataSourceName) {
        // 标识符：数据源名称 + schema + 表名
        String tableKey = dataSourceName + "|" + (schema != null ? schema : "") + "|" + tableName;
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
            
            // 获取当前数据库产品名称，用于识别系统表
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();
            
            ResultSet tablesResultSet = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            
            while (tablesResultSet.next()) {
                String tableName = tablesResultSet.getString("TABLE_NAME");
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
                        log.info("表[{}]中发现{}个金额字段: {}", tableName, tableInfo.getMoneyFields().size(), tableInfo.getMoneyFields());
                    }
                    
                    // 获取表记录数
                    try {
                        // 拼接带Schema的完整表名
                        String fullTableName = tableSchema != null && !tableSchema.isEmpty() 
                            ? tableSchema + "." + tableName 
                            : tableName;
                        
                        // 查询记录数
                        String countSql = "SELECT COUNT(*) FROM " + fullTableName;
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
                                .map(field -> String.format("SUM(%s) AS \"%s\"", field, field))
                                .collect(Collectors.toList());
                            
                            String sumSql = String.format("SELECT %s FROM %s",
                                String.join(", ", sumExpressions),
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
                                        log.warn("无法将 {} 转换为 BigDecimal: {}", value, e.getMessage());
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
                    log.warn("获取表[{}]的金额字段时出错: {}", tableName, e.getMessage(), e);
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
        if (includeTables == null || includeTables.trim().isEmpty()) {
            return false;
        }
        
        String fullName = tableName + "@" + schema;
        String[] tables = includeTables.split(",");
        
        for (String table : tables) {
            table = table.trim();
            if (table.isEmpty()) {
                continue;
            }
            
            if (table.contains("@")) {
                // 带schema的完整形式
                if (table.equalsIgnoreCase(fullName)) {
                    log.debug("表[{}]匹配包含列表中的[{}]", fullName, table);
                    return true;
                }
            } else if (table.equalsIgnoreCase(tableName)) {
                // 不带schema的简单形式
                log.debug("表[{}]匹配包含列表中的[{}]", tableName, table);
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 检查表名是否在排除列表中
     */
    private boolean isInExcludedTables(String tableName, String schema) {
        if (excludeTables == null || excludeTables.trim().isEmpty()) {
            return false;
        }
        
        String fullName = tableName + "@" + schema;
        String[] tables = excludeTables.split(",");
        
        for (String table : tables) {
            table = table.trim();
            if (table.isEmpty()) {
                continue;
            }
            
            if (table.contains("@")) {
                // 带schema的完整形式
                if (table.equalsIgnoreCase(fullName)) {
                    log.debug("表[{}]匹配排除列表中的[{}]", fullName, table);
                    return true;
                }
            } else if (table.equalsIgnoreCase(tableName)) {
                // 不带schema的简单形式
                log.debug("表[{}]匹配排除列表中的[{}]", tableName, table);
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 检查schema是否在包含列表中
     */
    private boolean isInIncludedSchemas(String schema) {
        if (includeSchemas == null || includeSchemas.trim().isEmpty()) {
            log.debug("包含的schema列表为空");
            return false;
        }
        
        String[] schemas = includeSchemas.split(",");
        for (String includedSchema : schemas) {
            includedSchema = includedSchema.trim();
            if (includedSchema.isEmpty()) {
                continue;
            }
            
            if (includedSchema.equalsIgnoreCase(schema)) {
                log.debug("Schema[{}]在包含列表中", schema);
                return true;
            }
        }
        
        log.debug("Schema[{}]不在包含列表[{}]中", schema, includeSchemas);
        return false;
    }
    
    /**
     * 查询表的记录数
     * 注意：此方法功能已被整合到getTablesInfoFromDataSource方法中，仅保留用于兼容现有代码
     * 
     * @param tableName 表名
     * @param schema 表所在的schema
     * @param dataSourceName 数据源名称
     * @param jdbcTemplate 数据源的JdbcTemplate
     * @return 表的记录数
     */
    private long getRecordCount(String tableName, String schema, String dataSourceName, JdbcTemplate jdbcTemplate) {
        try {
            long count = getTableRecordCount(jdbcTemplate, schema, tableName);
            log.info("表[{}]在{}中有{}条记录", tableName, dataSourceName, count);
            return count;
        } catch (Exception e) {
            log.error("获取表[{}]在{}中的记录数时出错: {}", tableName, dataSourceName, e.getMessage(),e);
            return 0L;
        }
    }

    /**
     * 查询表的记录数
     * 注意：此方法功能已被整合到getTablesInfoFromDataSource方法中，仅保留用于兼容现有代码
     * 
     * @param jdbcTemplate 数据源的JdbcTemplate
     * @param schema 表所在的schema
     * @param tableName 表名
     * @return 表的记录数
     */
    private long getTableRecordCount(JdbcTemplate jdbcTemplate, String schema, String tableName) {
        try {
            // 拼接带Schema的完整表名
            String fullTableName = schema != null && !schema.isEmpty() 
                ? schema + "." + tableName 
                : tableName;
            
            // 查询记录数
            String sql = "SELECT COUNT(*) FROM " + fullTableName;
            log.debug("执行SQL: {}", sql);
            Long count = jdbcTemplate.queryForObject(sql, Long.class);
            return count != null ? count : 0L;
        } catch (Exception e) {
            log.error("获取表[{}]记录数时出错: {}", tableName, e.getMessage(),e);
            return 0L;
        }
    }

    /**
     * 将表信息添加到映射中，使用表名+SCHEMA作为唯一标识
     */
    private void addTableInfoToMap(Map<String, TableMetaInfo> tableInfoMap, List<TableInfo> tables, String sourceName, JdbcTemplate jdbcTemplate) {
        log.info("开始处理{}的{}个表信息...", sourceName, tables.size());
        
        tables.forEach(table -> {
            // 创建唯一键，使用表名和schema，让表名排在前面有利于按表名排序
            String key = table.getTableName() + "#@#" + table.getSchema();
            
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
                            table.getMoneyFields().size(), String.join(", ", table.getMoneyFields()));
                    }
                } catch (Exception e) {
                    log.warn("处理表 {}.{} 的金额字段时出错: {}", table.getSchema(), table.getTableName(), e.getMessage(), e);
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
                counts.add(String.valueOf(count));
            }
            return String.join("|", counts);
        }
        
        public void addMoneyField(String fieldName) {
            moneyFields.add(fieldName);
        }
        
        public String getFormattedMoneyFields() {
            if (moneyFields.isEmpty()) {
                return "";
            }
            List<String> sortedFields = new ArrayList<>(moneyFields);
            Collections.sort(sortedFields);
            return String.join("|", sortedFields);
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
        if (!StringUtils.isEmpty(rerunDatabases)) {
            String[] dbs = rerunDatabases.split(",");
            for (String db : dbs) {
                String dbName = db.trim();
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
        if (!StringUtils.isEmpty(rerunDatabases)) {
            log.info("指定重跑数据库: {}", rerunDatabases);
        }
        log.info("开始收集表信息...");
        
        // 创建一个Map存储表的基本信息
        Map<String, TableMetaInfo> tableInfoMap = new HashMap<>();
        
        // 获取需要处理的数据库
        Map<String, JdbcTemplate> databasesToProcess = getDatabasesToProcess();
        log.info("本次将处理{}个数据库: {}", databasesToProcess.size(), String.join(", ", databasesToProcess.keySet()));
        
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
                
                if (!tables.isEmpty()) {
                    TableInfo firstTable = tables.get(0);
                    String dataSourceName = firstTable.getDataSourceName();
                    
                    // 添加表信息到映射
                    addTableInfoToMap(tableInfoMap, tables, dataSourceName, null);
                }
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
        
        // 将tableInfoMap转换为展开后的MoneyFieldSumInfo列表
        List<MoneyFieldSumInfo> expandedSumInfoList = new ArrayList<>();
        
        // 对表名进行字母排序
        List<String> sortedKeys = new ArrayList<>(tableInfoMap.keySet());
        Collections.sort(sortedKeys);
        
        log.info("开始计算各表金额字段SUM值...");
        
        // 创建一个临时Map用于按表名分组
        Map<String, List<MoneyFieldSumInfo>> tableNameGroupMap = new HashMap<>();
        
        // 使用TableMetaInfo中的求和结果，直接构建输出信息
        sortedKeys.forEach(key -> {
            String[] parts = key.split("#@#", 2);
            String tableName = parts[0];
            
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
                        metaInfo.getSchema(),
                        String.join(" | ", metaInfo.getDataSources()),
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
        
        // 按表名排序，并对每个表内的记录按金额字段名排序，然后添加到结果列表
        tableNameGroupMap.keySet().stream()
            .sorted()
            .forEach(tableName -> {
                List<MoneyFieldSumInfo> tableInfos = tableNameGroupMap.get(tableName);
                tableInfos.sort(Comparator.comparing(MoneyFieldSumInfo::getSumField));
                expandedSumInfoList.addAll(tableInfos);
            });
        
        // 导出到Excel
        exportDynamicExcel(outputPath, expandedSumInfoList);
        
        log.info("金额字段SUM比对结果已成功导出到: {}", outputPath);
        
        // 在最后，确保所有处理都被标记为已完成
        saveResumeState();
    }

    /**
     * 批量计算金额字段的SUM值
     * 注意：此方法功能已被整合到getTablesInfoFromDataSource方法中，仅保留用于兼容现有代码
     * 
     * @param jdbcTemplate 数据源的JdbcTemplate
     * @param schema 表所在的schema
     * @param tableName 表名
     * @param fieldNames 金额字段列表
     * @return 字段名到SUM值的映射
     */
    private Map<String, BigDecimal> calculateSumBatch(JdbcTemplate jdbcTemplate, String schema, String tableName, List<String> fieldNames) {
        Map<String, BigDecimal> results = new HashMap<>();
        if (fieldNames == null || fieldNames.isEmpty()) {
            return results;
        }
        
        try {
            // 构建查询语句，一次查询所有字段的SUM
            List<String> sumExpressions = fieldNames.stream()
                .map(field -> StrUtil.format("SUM({}) AS \"{}\"", field, field))
                .collect(Collectors.toList());
            
            String sql = StrUtil.format("SELECT {} FROM {}",
                StrUtil.join(", ", sumExpressions),
                tableName);
            
            log.info("执行批量SUM查询: {}", sql);
            
            // 执行查询并映射结果
            Map<String, Object> resultMap = jdbcTemplate.queryForMap(sql);
            for (String fieldName : fieldNames) {
                Object value = resultMap.get(fieldName);
                BigDecimal decimalValue = null;
                if (value instanceof BigDecimal) {
                    decimalValue = (BigDecimal) value;
                } else if (value != null) {
                    try {
                        decimalValue = new BigDecimal(value.toString());
                    } catch (NumberFormatException e) {
                        log.warn("无法将 {} 转换为 BigDecimal: {}", value, e.getMessage());
                    }
                }
                results.put(fieldName, decimalValue != null ? decimalValue : BigDecimal.ZERO);
                log.debug("表 {}.{} 字段 {} 的SUM值为: {}", schema, tableName, fieldName, decimalValue);
            }
        } catch (Exception e) {
            log.warn("批量计算表 {}.{} 的SUM值时出错: {}", schema, tableName, e.getMessage(),e);
        }
        return results;
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
            
            // 创建表头行
            Row headerRow = sheet.createRow(0);
            
            // 固定列的标题
            String[] fixedHeaders = {"表名", "SCHEMA", "所在库", "COUNT_ORA", "COUNT_RLCMS_BASE", "COUNT_RLCMS_PV1", "COUNT_RLCMS_PV2", "COUNT_RLCMS_PV3", "COUNT_BSCOPY_PV1", "COUNT_BSCOPY_PV2", "COUNT_BSCOPY_PV3", "金额字段", "SUM字段", "SUM_ORA", "SUM_RLCMS_BASE", "SUM_RLCMS_PV1", "SUM_RLCMS_PV2", "SUM_RLCMS_PV3", "SUM_BSCOPY_PV1", "SUM_BSCOPY_PV2", "SUM_BSCOPY_PV3"};
            
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
     * 对单个数据库进行并发查询，查询多个表
     * @param jdbcTemplate 数据库连接
     * @param dataSourceName 数据源名称
     * @param tableNames 要查询的表名列表
     * @param schema 模式名
     * @param tableToSchemaMap 表名到schema的映射，当schema为null时使用
     * @return 每个表的字段SUM结果
     */
    public Map<String, Map<String, BigDecimal>> concurrentQuerySingleDatabase(
            JdbcTemplate jdbcTemplate, 
            String dataSourceName, 
            List<String> tableNames, 
            String schema,
            Map<String, String> tableToSchemaMap) {
        
        log.info("开始并发查询数据源[{}]的{}个表", dataSourceName, tableNames.size());
        
        // 创建结果Map
        ConcurrentHashMap<String, Map<String, BigDecimal>> results = new ConcurrentHashMap<>();
        
        // 创建线程池
        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors() * 2, 16)
        );
        
        // 创建查询任务
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (String tableName : tableNames) {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    // 确定要使用的schema
                    String tableSchema = schema;
                    if (tableSchema == null && tableToSchemaMap != null) {
                        tableSchema = tableToSchemaMap.get(tableName);
                    }
                    
                    if (tableSchema == null) {
                        log.warn("无法确定表[{}]的schema，跳过查询", tableName);
                        return;
                    }
                    
                    // 获取金额字段
                    List<String> moneyFields = new ArrayList<>();
                    try (Connection connection = jdbcTemplate.getDataSource().getConnection()) {
                        DatabaseMetaData metaData = connection.getMetaData();
                        
                        // 获取列信息
                        try (ResultSet columns = metaData.getColumns(null, tableSchema, tableName, null)) {
                            while (columns.next()) {
                                String columnName = columns.getString("COLUMN_NAME");
                                int dataType = columns.getInt("DATA_TYPE");
                                int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                                
                                // 判断是否为金额字段：数值型且小数位不为0
                                if (isNumericType(dataType) && decimalDigits > 0) {
                                    moneyFields.add(columnName);
                                    log.debug("发现金额字段: {}.{}.{}, 类型: {}, 小数位: {}", 
                                            tableSchema, tableName, columnName, dataType, decimalDigits);
                                }
                            }
                        }
                        
                        if (!moneyFields.isEmpty()) {
                            log.info("表[{}]中发现{}个金额字段: {}", tableName, moneyFields.size(), moneyFields);
                        }
                    } catch (SQLException e) {
                        log.warn("获取表[{}]的金额字段时出错: {}", tableName, e.getMessage(), e);
                    }
                    
                    if (!moneyFields.isEmpty()) {
                        // 计算SUM值
                        Map<String, BigDecimal> sums = calculateSumBatch(jdbcTemplate, tableSchema, tableName, moneyFields);
                        results.put(tableName, sums);
                        log.info("数据源[{}]表[{}.{}]查询完成，有{}个金额字段", dataSourceName, tableSchema, tableName, moneyFields.size());
                    } else {
                        log.info("数据源[{}]表[{}.{}]没有金额字段，跳过", dataSourceName, tableSchema, tableName);
                    }
                } catch (Exception e) {
                    log.error("查询数据源[{}]表[{}]出错: {}", dataSourceName, tableName, e.getMessage(),e);
                }
            }, executor);
            
            futures.add(future);
        }
        
        // 等待所有任务完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        // 关闭线程池
        executor.shutdown();
        try {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        log.info("数据源[{}]的{}个表查询完成", dataSourceName, results.size());
        return results;
    }

    /**
     * 对单个数据库进行并发查询，查询多个表
     * @param jdbcTemplate 数据库连接
     * @param dataSourceName 数据源名称
     * @param tableNames 要查询的表名列表
     * @param schema 模式名
     * @return 每个表的字段SUM结果
     */
    public Map<String, Map<String, BigDecimal>> concurrentQuerySingleDatabase(
            JdbcTemplate jdbcTemplate, 
            String dataSourceName, 
            List<String> tableNames, 
            String schema) {
        return concurrentQuerySingleDatabase(jdbcTemplate, dataSourceName, tableNames, schema, null);
    }

    /**
     * 并发查询多个数据库
     * @param datasources 数据源Map，key为数据源名称，value为JdbcTemplate
     * @param tableNames 要查询的表名列表
     * @param schema 模式名
     * @param tableToSchemaMap 表名到schema的映射，当schema为null时使用
     * @return 每个数据源的查询结果
     */
    public Map<String, Map<String, Map<String, BigDecimal>>> concurrentQueryMultipleDatabases(
            Map<String, JdbcTemplate> datasources, 
            List<String> tableNames, 
            String schema,
            Map<String, String> tableToSchemaMap) {
        
        log.info("开始并发查询{}个数据源的{}个表", datasources.size(), tableNames.size());
        
        // 创建结果Map
        ConcurrentHashMap<String, Map<String, Map<String, BigDecimal>>> results = new ConcurrentHashMap<>();
        
        // 创建查询任务
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (Map.Entry<String, JdbcTemplate> entry : datasources.entrySet()) {
            String dataSourceName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();
            
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                Map<String, Map<String, BigDecimal>> dbResult = concurrentQuerySingleDatabase(
                    jdbcTemplate, dataSourceName, tableNames, schema, tableToSchemaMap);
                results.put(dataSourceName, dbResult);
                return null;
            });
            
            futures.add(future);
        }
        
        // 等待所有任务完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        log.info("所有{}个数据源的查询完成", datasources.size());
        return results;
    }

    /**
     * 并发查询多个数据库
     * @param datasources 数据源Map，key为数据源名称，value为JdbcTemplate
     * @param tableNames 要查询的表名列表
     * @param schema 模式名
     * @return 每个数据源的查询结果
     */
    public Map<String, Map<String, Map<String, BigDecimal>>> concurrentQueryMultipleDatabases(
            Map<String, JdbcTemplate> datasources, 
            List<String> tableNames, 
            String schema) {
        return concurrentQueryMultipleDatabases(datasources, tableNames, schema, null);
    }

    /**
     * 导出多个数据库并发查询结果到Excel
     * 这是一个示例方法，展示如何使用并发查询功能
     */
    public void exportConcurrentQueryResultToExcel() throws IOException {
        log.info("开始并发查询导出...");
        
        // 准备数据源
        Map<String, JdbcTemplate> datasources = new HashMap<>();
        datasources.put("ora", oraJdbcTemplate);
        datasources.put("rlcms_base", rlcmsBaseJdbcTemplate);
        datasources.put("rlcms_pv1", rlcmsPv1JdbcTemplate);
        datasources.put("rlcms_pv2", rlcmsPv2JdbcTemplate);
        datasources.put("rlcms_pv3", rlcmsPv3JdbcTemplate);
        datasources.put("bscopy_pv1", bscopyPv1JdbcTemplate);
        datasources.put("bscopy_pv2", bscopyPv2JdbcTemplate);
        datasources.put("bscopy_pv3", bscopyPv3JdbcTemplate);
        
        // 第一步：并发获取所有数据源的表信息
        log.info("开始收集表信息...");
        Map<String, List<TableInfo>> allTableInfo = new ConcurrentHashMap<>();
        
        List<CompletableFuture<Void>> tableFutures = new ArrayList<>();
        
        datasources.forEach((dataSourceName, jdbcTemplate) -> {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                List<TableInfo> tables = getTablesInfoFromDataSource(jdbcTemplate, dataSourceName);
                log.info("从{}数据源获取到{}个表", dataSourceName, tables.size());
                allTableInfo.put(dataSourceName, tables);
                return null;
            });
            
            tableFutures.add(future);
        });
        
        // 等待所有获取表信息的任务完成
        CompletableFuture.allOf(tableFutures.toArray(new CompletableFuture[0])).join();

        // 汇总所有表信息
        Map<String, TableMetaInfo> tableInfoMap = new HashMap<>();
        allTableInfo.forEach((dataSourceName, tables) -> {
            JdbcTemplate jdbcTemplate = datasources.get(dataSourceName);
            addTableInfoToMap(tableInfoMap, tables, dataSourceName, jdbcTemplate);
        });
        
        log.info("表信息收集完成，共发现{}个表", tableInfoMap.size());
        
        // 第二步：提取所有需要查询的表名
        List<String> allTablesToQuery = new ArrayList<>();
        Map<String, String> tableToSchemaMap = new HashMap<>(); // 保存表对应的schema
        
        tableInfoMap.forEach((key, metaInfo) -> {
            String[] parts = key.split("#@#", 2);
            String tableName = parts[0];
            
            // 只查询有金额字段的表
            if (!metaInfo.getMoneyFields().isEmpty() && !allTablesToQuery.contains(tableName)) {
                allTablesToQuery.add(tableName);
                tableToSchemaMap.put(tableName, metaInfo.getSchema());
            }
        });
        
        log.info("需要查询的表总数: {}", allTablesToQuery.size());
        
        // 第三步：并发查询所有数据库的所有表，使用tableToSchemaMap来确定每个表的schema
        Map<String, Map<String, Map<String, BigDecimal>>> queryResults = 
            concurrentQueryMultipleDatabases(datasources, allTablesToQuery, null, tableToSchemaMap);
        
        // 第四步：整理数据并导出到Excel
        // 创建结果目录
        File directory = new File(exportDirectory);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        
        String outputPath = exportDirectory + File.separator + "并发查询SUM比对-" 
                + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + ".xlsx";
        
        // 将查询结果转换为导出格式 - 按表名分组处理数据
        Map<String, List<MoneyFieldSumInfo>> tableNameGroupMap = new HashMap<>();
        
        // 处理所有表的数据，按表名分组
        allTablesToQuery.forEach(tableName -> {
            String schema = tableToSchemaMap.get(tableName);
            TableMetaInfo metaInfo = tableInfoMap.get(tableName + "#@#" + schema);
            
            if (metaInfo == null || metaInfo.getMoneyFields().isEmpty()) {
                return; // 跳过没有金额字段的表
            }
            
            // 对金额字段排序并创建MoneyFieldSumInfo
            metaInfo.getMoneyFields().stream()
                .sorted()
                .forEach(moneyField -> {
                    MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                        tableName,
                        schema,
                        String.join(" | ", metaInfo.getDataSources()),
                        metaInfo.getFormattedRecordCounts(),
                        metaInfo.getFormattedMoneyFields(),
                        moneyField
                    );
                    
                    // 设置各数据源的记录数和SUM值
                    IntStream.range(0, datasources.size()).forEach(i -> {
                        String dataSourceName = (String) datasources.keySet().toArray()[i];
                        
                        // 设置记录数
                        if (metaInfo.getDataSources().contains(dataSourceName)) {
                            Long count = metaInfo.getRecordCounts().getOrDefault(dataSourceName, 0L);
                            sumInfo.setCountValue(i+1, count);
                            
                            // 设置SUM值
                            Map<String, Map<String, BigDecimal>> dataSourceResults = queryResults.get(dataSourceName);
                            if (dataSourceResults != null) {
                                Map<String, BigDecimal> tableSums = dataSourceResults.get(tableName);
                                if (tableSums != null) {
                                    BigDecimal sum = tableSums.getOrDefault(moneyField, BigDecimal.ZERO);
                                    sumInfo.setSumValue(i+1, sum);
                                }
                            }
                        }
                    });
                    
                    // 添加到表名分组
                    tableNameGroupMap.computeIfAbsent(tableName, k -> new ArrayList<>()).add(sumInfo);
                });
        });
        
        // 创建并填充结果列表，按表名排序，同一表内按金额字段排序
        List<MoneyFieldSumInfo> expandedSumInfoList = tableNameGroupMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .flatMap(entry -> entry.getValue().stream()
                .sorted(Comparator.comparing(MoneyFieldSumInfo::getSumField)))
            .collect(Collectors.toList());
        
        // 导出到Excel
        exportDynamicExcel(outputPath, expandedSumInfoList);
        
        log.info("并发查询SUM比对结果已成功导出到: {}", outputPath);
    }
} 