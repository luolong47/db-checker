package io.github.luolong47.dbchecker.service;

import cn.hutool.core.util.StrUtil;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
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

    public DatabaseService(
            @Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate,
            @Qualifier("rlcmsBaseJdbcTemplate") JdbcTemplate rlcmsBaseJdbcTemplate,
            @Qualifier("rlcmsPv1JdbcTemplate") JdbcTemplate rlcmsPv1JdbcTemplate,
            @Qualifier("rlcmsPv2JdbcTemplate") JdbcTemplate rlcmsPv2JdbcTemplate,
            @Qualifier("rlcmsPv3JdbcTemplate") JdbcTemplate rlcmsPv3JdbcTemplate,
            @Qualifier("bscopyPv1JdbcTemplate") JdbcTemplate bscopyPv1JdbcTemplate,
            @Qualifier("bscopyPv2JdbcTemplate") JdbcTemplate bscopyPv2JdbcTemplate,
            @Qualifier("bscopyPv3JdbcTemplate") JdbcTemplate bscopyPv3JdbcTemplate) {
        this.oraJdbcTemplate = oraJdbcTemplate;
        this.rlcmsBaseJdbcTemplate = rlcmsBaseJdbcTemplate;
        this.rlcmsPv1JdbcTemplate = rlcmsPv1JdbcTemplate;
        this.rlcmsPv2JdbcTemplate = rlcmsPv2JdbcTemplate;
        this.rlcmsPv3JdbcTemplate = rlcmsPv3JdbcTemplate;
        this.bscopyPv1JdbcTemplate = bscopyPv1JdbcTemplate;
        this.bscopyPv2JdbcTemplate = bscopyPv2JdbcTemplate;
        this.bscopyPv3JdbcTemplate = bscopyPv3JdbcTemplate;
    }

    /**
     * 从数据源获取表信息
     */
    private List<TableInfo> getTablesInfoFromDataSource(JdbcTemplate jdbcTemplate, String dataSourceName) {
        log.info("开始获取数据源[{}]的表信息", dataSourceName);
        log.info("当前配置 - 包含Schema: [{}], 包含表: [{}], 排除表: [{}]", 
                includeSchemas != null ? includeSchemas : "空", 
                includeTables != null ? includeTables : "空", 
                excludeTables != null ? excludeTables : "空");
        
        List<TableInfo> tables = new ArrayList<>();
        try {
            DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
            
            // 获取当前数据库产品名称，用于识别系统表
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();
            
            ResultSet tablesResultSet = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            
            while (tablesResultSet.next()) {
                String tableName = tablesResultSet.getString("TABLE_NAME");
                String tableSchema = tablesResultSet.getString("TABLE_SCHEM");

                // 判断是否应该排除该表
                if (shouldExcludeTable(tableName, tableSchema)) {
                    continue;
                }
                
                TableInfo tableInfo = new TableInfo(tableName, tableSchema != null ? tableSchema : "");
                tables.add(tableInfo);
            }
            
            log.info("从{}获取到{}个非系统表", dataSourceName, tables.size());
            return tables;
        } catch (SQLException e) {
            log.error("获取{}的表信息时出错: {}", dataSourceName, e.getMessage(),e);
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
     * 
     * @param jdbcTemplate 数据源的JdbcTemplate
     * @param schema 表所在的schema
     * @param tableName 表名
     * @return 表记录数
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
            log.warn("查询表[{}]记录数时出错: {}", tableName, e.getMessage(),e);
            return 0L;
        }
    }

    /**
     * 获取表在指定数据源中的记录数
     * 
     * @param tableName 表名
     * @param schema 表所在的schema
     * @param dataSourceName 数据源名称
     * @param jdbcTemplate 数据源的JdbcTemplate
     * @return 记录数
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
     * 将表信息添加到映射中，使用表名+SCHEMA作为唯一标识
     */
    private void addTableInfoToMap(Map<String, TableMetaInfo> tableInfoMap, List<TableInfo> tables, String sourceName, JdbcTemplate jdbcTemplate) {
        log.info("开始处理{}的{}个表信息...", sourceName, tables.size());
        for (TableInfo table : tables) {
            // 创建唯一键，使用表名和schema，但使用一个不太可能出现在表名或schema中的分隔符
            // 修改为使用#@#作为分隔符而不是|||
            String key = table.getTableName() + "#@#" + table.getSchema();
            
            // 获取或创建TableMetaInfo
            TableMetaInfo metaInfo = tableInfoMap.computeIfAbsent(key, k -> new TableMetaInfo(table.getSchema()));
            
            // 添加数据源
            metaInfo.addDataSource(sourceName);
            
            // 获取并设置记录数
            long recordCount = getRecordCount(table.getTableName(), table.getSchema(), sourceName, jdbcTemplate);
            metaInfo.setRecordCount(sourceName, recordCount);
            
            // 获取并添加金额字段（只需获取一次）
            if (metaInfo.getMoneyFields().isEmpty()) {
                try {
                    List<String> moneyFields = getMoneyFields(jdbcTemplate, table.getSchema(), table.getTableName());
                    for (String field : moneyFields) {
                        metaInfo.addMoneyField(field);
                    }
                    if (!moneyFields.isEmpty()) {
                        log.info("表 {}.{} 发现{}个金额字段: {}", 
                            table.getSchema(), table.getTableName(), 
                            moneyFields.size(), String.join(", ", moneyFields));
                    }
                } catch (Exception e) {
                    log.warn("获取表 {}.{} 的金额字段时出错: {}", table.getSchema(), table.getTableName(), e.getMessage(),e);
                }
            }
        }
        log.info("{}的表信息处理完成", sourceName);
    }
    
    /**
     * 表信息类
     */
    @Data
    private static class TableInfo {
        private final String tableName;
        private final String schema;
        
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
    }

    /**
     * 获取表中的金额字段列表（数值型且小数位不为0的字段）
     * 
     * @param jdbcTemplate 数据源的JdbcTemplate
     * @param schema 表所在的schema
     * @param tableName 表名
     * @return 金额字段列表
     */
    private List<String> getMoneyFields(JdbcTemplate jdbcTemplate, String schema, String tableName) {
        List<String> moneyFields = new ArrayList<>();
        
        try {
            DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
            
            // 获取列信息
            try (ResultSet columns = metaData.getColumns(null, schema, tableName, null)) {
                while (columns.next()) {
                    String columnName = columns.getString("COLUMN_NAME");
                    int dataType = columns.getInt("DATA_TYPE");
                    int decimalDigits = columns.getInt("DECIMAL_DIGITS");
                    
                    // 判断是否为金额字段：数值型且小数位不为0
                    if (isNumericType(dataType) && decimalDigits > 0) {
                        moneyFields.add(columnName);
                        log.debug("发现金额字段: {}.{}.{}, 类型: {}, 小数位: {}", 
                                 schema, tableName, columnName, dataType, decimalDigits);
                    }
                }
            }
            
            if (!moneyFields.isEmpty()) {
                log.info("表[{}]中发现{}个金额字段: {}", tableName, moneyFields.size(), moneyFields);
            }
            
            return moneyFields;
        } catch (SQLException e) {
            log.warn("获取表[{}]的金额字段时出错: {}", tableName, e.getMessage(),e);
            return Collections.emptyList();
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
     * 导出表名和金额字段SUM结果信息到Excel
     */
    public void exportMoneyFieldSumToExcel() throws IOException {
        log.info("开始收集表信息...");
        
        // 创建一个Map存储表的基本信息
        Map<String, TableMetaInfo> tableInfoMap = new HashMap<>();
        
        // 使用CompletableFuture并发获取表信息
        CompletableFuture<List<TableInfo>> oraFuture = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(oraJdbcTemplate, "ora");
            log.info("从ora数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> rlcmsBaseFuture = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(rlcmsBaseJdbcTemplate, "rlcms_base");
            log.info("从rlcms_base数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> rlcmsPv1Future = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(rlcmsPv1JdbcTemplate, "rlcms_pv1");
            log.info("从rlcms_pv1数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> rlcmsPv2Future = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(rlcmsPv2JdbcTemplate, "rlcms_pv2");
            log.info("从rlcms_pv2数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> rlcmsPv3Future = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(rlcmsPv3JdbcTemplate, "rlcms_pv3");
            log.info("从rlcms_pv3数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> bscopyPv1Future = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(bscopyPv1JdbcTemplate, "bscopy_pv1");
            log.info("从bscopy_pv1数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> bscopyPv2Future = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(bscopyPv2JdbcTemplate, "bscopy_pv2");
            log.info("从bscopy_pv2数据源获取到{}个表", tables.size());
            return tables;
        });
        
        CompletableFuture<List<TableInfo>> bscopyPv3Future = CompletableFuture.supplyAsync(() -> {
            List<TableInfo> tables = getTablesInfoFromDataSource(bscopyPv3JdbcTemplate, "bscopy_pv3");
            log.info("从bscopy_pv3数据源获取到{}个表", tables.size());
            return tables;
        });
        
        // 等待所有Future完成
        CompletableFuture.allOf(
            oraFuture, rlcmsBaseFuture, rlcmsPv1Future, rlcmsPv2Future, rlcmsPv3Future,
            bscopyPv1Future, bscopyPv2Future, bscopyPv3Future
        ).join();
        
        // 将结果添加到tableInfoMap
        try {
            addTableInfoToMap(tableInfoMap, oraFuture.get(), "ora", oraJdbcTemplate);
            addTableInfoToMap(tableInfoMap, rlcmsBaseFuture.get(), "rlcms_base", rlcmsBaseJdbcTemplate);
            addTableInfoToMap(tableInfoMap, rlcmsPv1Future.get(), "rlcms_pv1", rlcmsPv1JdbcTemplate);
            addTableInfoToMap(tableInfoMap, rlcmsPv2Future.get(), "rlcms_pv2", rlcmsPv2JdbcTemplate);
            addTableInfoToMap(tableInfoMap, rlcmsPv3Future.get(), "rlcms_pv3", rlcmsPv3JdbcTemplate);
            addTableInfoToMap(tableInfoMap, bscopyPv1Future.get(), "bscopy_pv1", bscopyPv1JdbcTemplate);
            addTableInfoToMap(tableInfoMap, bscopyPv2Future.get(), "bscopy_pv2", bscopyPv2JdbcTemplate);
            addTableInfoToMap(tableInfoMap, bscopyPv3Future.get(), "bscopy_pv3", bscopyPv3JdbcTemplate);
        } catch (InterruptedException | ExecutionException e) {
            log.error("并发获取表信息时发生错误", e);
            Thread.currentThread().interrupt();
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
        
        // 创建线程池用于并发计算SUM值
        ExecutorService executor = Executors.newFixedThreadPool(
            Math.min(Runtime.getRuntime().availableProcessors() * 2, 16)
        );
        
        List<CompletableFuture<Void>> sumFutures = new ArrayList<>();
        
        for (String key : sortedKeys) {
            String[] parts = key.split("#@#", 2);
            String tableName = parts[0];
            
            TableMetaInfo metaInfo = tableInfoMap.get(key);
            
            // 对于每个表，如果有金额字段，则为每个金额字段创建一个MoneyFieldSumInfo
            if (!metaInfo.getMoneyFields().isEmpty()) {
                List<String> sortedMoneyFields = new ArrayList<>(metaInfo.getMoneyFields());
                Collections.sort(sortedMoneyFields);
                
                // 存储各数据源的批量SUM查询结果
                ConcurrentHashMap<String, Map<String, BigDecimal>> datasourceSums = new ConcurrentHashMap<>();
                
                // 并发执行各数据源的SUM查询
                List<CompletableFuture<Void>> tableSumFutures = new ArrayList<>();
                
                if (metaInfo.getDataSources().contains("ora")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(oraJdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("ora", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("rlcms_base")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(rlcmsBaseJdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("rlcms_base", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("rlcms_pv1")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(rlcmsPv1JdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("rlcms_pv1", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("rlcms_pv2")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(rlcmsPv2JdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("rlcms_pv2", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("rlcms_pv3")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(rlcmsPv3JdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("rlcms_pv3", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("bscopy_pv1")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(bscopyPv1JdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("bscopy_pv1", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("bscopy_pv2")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(bscopyPv2JdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("bscopy_pv2", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                if (metaInfo.getDataSources().contains("bscopy_pv3")) {
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Map<String, BigDecimal> sums = calculateSumBatch(bscopyPv3JdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                        datasourceSums.put("bscopy_pv3", sums);
                    }, executor);
                    tableSumFutures.add(future);
                }
                
                // 等待当前表的所有SUM查询完成后处理结果
                CompletableFuture<Void> tableProcessFuture = CompletableFuture.allOf(
                    tableSumFutures.toArray(new CompletableFuture[0])
                ).thenAccept(v -> {
                    // 为每个金额字段创建MoneyFieldSumInfo并设置SUM值
                    for (String moneyField : sortedMoneyFields) {
                        MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                            tableName,
                            metaInfo.getSchema(),
                            String.join(" | ", metaInfo.getDataSources()),
                            metaInfo.getFormattedRecordCounts(),
                            metaInfo.getFormattedMoneyFields(),
                            moneyField
                        );
                        
                        // 设置各数据源的记录数
                        if (metaInfo.getDataSources().contains("ora")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("ora", 0L);
                            sumInfo.setCountValue(1, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_base")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("rlcms_base", 0L);
                            sumInfo.setCountValue(2, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_pv1")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("rlcms_pv1", 0L);
                            sumInfo.setCountValue(3, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_pv2")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("rlcms_pv2", 0L);
                            sumInfo.setCountValue(4, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_pv3")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("rlcms_pv3", 0L);
                            sumInfo.setCountValue(5, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("bscopy_pv1")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("bscopy_pv1", 0L);
                            sumInfo.setCountValue(6, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("bscopy_pv2")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("bscopy_pv2", 0L);
                            sumInfo.setCountValue(7, count);
                        }
                        
                        if (metaInfo.getDataSources().contains("bscopy_pv3")) {
                            Long count = metaInfo.getRecordCounts().getOrDefault("bscopy_pv3", 0L);
                            sumInfo.setCountValue(8, count);
                        }
                        
                        // 设置各数据源的SUM值
                        if (metaInfo.getDataSources().contains("ora")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("ora", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(1, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_base")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("rlcms_base", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(2, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_pv1")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("rlcms_pv1", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(3, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_pv2")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("rlcms_pv2", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(4, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("rlcms_pv3")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("rlcms_pv3", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(5, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("bscopy_pv1")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("bscopy_pv1", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(6, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("bscopy_pv2")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("bscopy_pv2", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(7, sum);
                        }
                        
                        if (metaInfo.getDataSources().contains("bscopy_pv3")) {
                            Map<String, BigDecimal> sums = datasourceSums.getOrDefault("bscopy_pv3", Collections.emptyMap());
                            BigDecimal sum = sums.getOrDefault(moneyField, BigDecimal.ZERO);
                            sumInfo.setSumValue(8, sum);
                        }
                        
                        synchronized (expandedSumInfoList) {
                            expandedSumInfoList.add(sumInfo);
                        }
                    }
                });
                
                sumFutures.add(tableProcessFuture);
            }
        }
        
        // 等待所有表的处理完成
        CompletableFuture.allOf(sumFutures.toArray(new CompletableFuture[0])).join();
        
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
        
        exportDynamicExcel(outputPath, expandedSumInfoList);
        
        log.info("金额字段SUM比对结果已成功导出到: {}", outputPath);
    }

    /**
     * 批量计算指定表的多个金额字段的SUM值
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
            for (int i = 0; i < fixedHeaders.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(fixedHeaders[i]);
                cell.setCellStyle(headerStyle);
            }
            
            // 填充数据
            int rowNum = 1;
            for (MoneyFieldSumInfo info : dataList) {
                Row row = sheet.createRow(rowNum++);
                
                // 设置固定列的值
                row.createCell(0).setCellValue(info.getTableName());
                row.createCell(1).setCellValue(info.getSchema());
                row.createCell(2).setCellValue(info.getDataSources());
                // 设置COUNT值
                for (int i = 1; i <= 8; i++) {
                    Long countValue = info.getCountValue(i);
                    if (countValue != null) {
                        Cell cell = row.createCell(2 + i);
                        cell.setCellValue(countValue);
                    } else {
                        row.createCell(2 + i).setCellValue("");
                    }
                }

                row.createCell(11).setCellValue(info.getMoneyFields());
                row.createCell(12).setCellValue(info.getSumField());
                
                
                // 设置SUM值（使用金额格式）
                for (int i = 1; i <= 8; i++) {
                    BigDecimal sumValue = info.getSumValue(i);
                    if (sumValue != null) {
                        Cell cell = row.createCell(12 + i);
                        cell.setCellValue(sumValue.doubleValue());
                        cell.setCellStyle(numberStyle);
                    } else {
                        row.createCell(12 + i).setCellValue("");
                    }
                }
            }
            
            // 自动调整列宽
            for (int i = 0; i < fixedHeaders.length; i++) {
                sheet.autoSizeColumn(i);
            }
            
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
                    List<String> moneyFields = getMoneyFields(jdbcTemplate, tableSchema, tableName);
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
        
        for (Map.Entry<String, JdbcTemplate> entry : datasources.entrySet()) {
            String dataSourceName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();
            
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                List<TableInfo> tables = getTablesInfoFromDataSource(jdbcTemplate, dataSourceName);
                log.info("从{}数据源获取到{}个表", dataSourceName, tables.size());
                allTableInfo.put(dataSourceName, tables);
                return null;
            });
            
            tableFutures.add(future);
        }
        
        // 等待所有获取表信息的任务完成
        CompletableFuture.allOf(tableFutures.toArray(new CompletableFuture[0])).join();
        
        // 汇总所有表信息
        Map<String, TableMetaInfo> tableInfoMap = new HashMap<>();
        for (Map.Entry<String, List<TableInfo>> entry : allTableInfo.entrySet()) {
            String dataSourceName = entry.getKey();
            List<TableInfo> tables = entry.getValue();
            JdbcTemplate jdbcTemplate = datasources.get(dataSourceName);
            
            addTableInfoToMap(tableInfoMap, tables, dataSourceName, jdbcTemplate);
        }
        
        log.info("表信息收集完成，共发现{}个表", tableInfoMap.size());
        
        // 第二步：提取所有需要查询的表名
        List<String> allTablesToQuery = new ArrayList<>();
        Map<String, String> tableToSchemaMap = new HashMap<>(); // 保存表对应的schema
        
        for (Map.Entry<String, TableMetaInfo> entry : tableInfoMap.entrySet()) {
            String[] parts = entry.getKey().split("#@#", 2);
            String tableName = parts[0];
            TableMetaInfo metaInfo = entry.getValue();
            
            // 只查询有金额字段的表
            if (!metaInfo.getMoneyFields().isEmpty()) {
                if (!allTablesToQuery.contains(tableName)) {
                    allTablesToQuery.add(tableName);
                    tableToSchemaMap.put(tableName, metaInfo.getSchema());
                }
            }
        }
        
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
        
        // 将查询结果转换为导出格式
        List<MoneyFieldSumInfo> expandedSumInfoList = new ArrayList<>();
        
        for (String tableName : allTablesToQuery) {
            String schema = tableToSchemaMap.get(tableName);
            TableMetaInfo metaInfo = tableInfoMap.get(tableName + "#@#" + schema);
            
            if (metaInfo == null || metaInfo.getMoneyFields().isEmpty()) {
                continue;
            }
            
            // 对表的金额字段按字母排序
            List<String> sortedMoneyFields = new ArrayList<>(metaInfo.getMoneyFields());
            Collections.sort(sortedMoneyFields);
            
            // 为每个金额字段创建一条记录
            for (String moneyField : sortedMoneyFields) {
                MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                    tableName,
                    schema,
                    String.join(" | ", metaInfo.getDataSources()),
                    metaInfo.getFormattedRecordCounts(),
                    metaInfo.getFormattedMoneyFields(),
                    moneyField
                );
                
                // 设置各数据源的记录数和SUM值
                for (int i = 0; i < datasources.size(); i++) {
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
                }
                
                expandedSumInfoList.add(sumInfo);
            }
        }
        
        // 导出到Excel
        exportDynamicExcel(outputPath, expandedSumInfoList);
        
        log.info("并发查询SUM比对结果已成功导出到: {}", outputPath);
    }
} 