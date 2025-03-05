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
import java.util.stream.Collectors;

/**
 * 数据库服务类，用于测试多数据源
 */
@Slf4j
@Service
public class DatabaseService {

    private final JdbcTemplate primaryJdbcTemplate;
    private final JdbcTemplate secondaryJdbcTemplate;
    private final JdbcTemplate tertiaryJdbcTemplate;
    
    // 用户可配置的要排除的schema列表，使用逗号分隔
    @Value("${db.exclude.schemas:INFORMATION_SCHEMA}")
    private String excludeSchemas;
    
    // 用户可配置的要排除的表名前缀，使用逗号分隔
    @Value("${db.exclude.table-prefixes:SYS_,sys_}")
    private String excludeTablePrefixes;

    // 用户可配置的要明确包含的表名，即使它们符合排除条件，使用逗号分隔
    @Value("${db.include.tables:}")
    private String includeTables;

    // 输出文件的目录，默认为当前目录
    @Value("${db.export.directory:.}")
    private String exportDirectory;

    public DatabaseService(
            @Qualifier("primaryJdbcTemplate") JdbcTemplate primaryJdbcTemplate,
            @Qualifier("secondaryJdbcTemplate") JdbcTemplate secondaryJdbcTemplate,
            @Qualifier("tertiaryJdbcTemplate") JdbcTemplate tertiaryJdbcTemplate) {
        this.primaryJdbcTemplate = primaryJdbcTemplate;
        this.secondaryJdbcTemplate = secondaryJdbcTemplate;
        this.tertiaryJdbcTemplate = tertiaryJdbcTemplate;
        
        // 记录当前的表过滤配置
        log.info("数据库表过滤配置: ");
        log.info("  排除的SCHEMA: {}", excludeSchemas);
        log.info("  排除的表名前缀: {}", excludeTablePrefixes);
        log.info("  明确包含的表: {}", includeTables);
    }

    /**
     * 查询主数据源中的用户数据
     */
    public List<Map<String, Object>> queryUsersFromPrimary() {
        String sql = "SELECT * FROM users";
        List<Map<String, Object>> result = primaryJdbcTemplate.queryForList(sql);
        log.info("从主数据源查询到 {} 条用户数据", result.size());
        return result;
    }

    /**
     * 查询第二个数据源中的产品数据
     */
    public List<Map<String, Object>> queryProductsFromSecondary() {
        String sql = "SELECT * FROM products";
        List<Map<String, Object>> result = secondaryJdbcTemplate.queryForList(sql);
        log.info("从第二个数据源查询到 {} 条产品数据", result.size());
        return result;
    }

    /**
     * 查询第三个数据源中的订单数据
     */
    public List<Map<String, Object>> queryOrdersFromTertiary() {
        String sql = "SELECT * FROM orders";
        List<Map<String, Object>> result = tertiaryJdbcTemplate.queryForList(sql);
        log.info("从第三个数据源查询到 {} 条订单数据", result.size());
        return result;
    }

    /**
     * 从指定数据源获取所有表信息（排除系统表）
     */
    private List<TableInfo> getTablesInfoFromDataSource(JdbcTemplate jdbcTemplate, String dataSourceName) {
        try {
            DatabaseMetaData metaData = jdbcTemplate.getDataSource().getConnection().getMetaData();
            
            // 获取当前数据库产品名称，用于识别系统表
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();
            
            ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
            
            List<TableInfo> tableInfoList = new ArrayList<>();
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                String tableSchema = tables.getString("TABLE_SCHEM");

                // 排除系统表
                if (isSystemTable(tableName, tableSchema, dbProductName)) {
                    continue;
                }
                
                TableInfo tableInfo = new TableInfo(tableName, tableSchema != null ? tableSchema : "");
                tableInfoList.add(tableInfo);
            }
            
            log.info("从{}获取到{}个非系统表", dataSourceName, tableInfoList.size());
            return tableInfoList;
        } catch (SQLException e) {
            log.error("获取{}的表信息时出错: {}", dataSourceName, e.getMessage());
            return Collections.emptyList();
        }
    }
    
    /**
     * 判断是否为系统表
     */
    private boolean isSystemTable(String tableName, String schema, String dbProductName) {
        // 检查是否明确要包含的表
        if (isInIncludedTables(tableName, schema)) {
            return false;
        }
        
        // 检查是否在用户配置的排除schema列表中
        if (schema != null && isInExcludedSchemas(schema)) {
            return true;
        }
        
        // 检查表名是否以排除的前缀开头
        if (hasExcludedPrefix(tableName)) {
            return true;
        }
        
        // 不同数据库的系统表判断规则
        if (dbProductName.contains("mysql")) {
            return tableName.startsWith("sys_") || 
                   "mysql".equalsIgnoreCase(schema) ||
                   "performance_schema".equalsIgnoreCase(schema);
        } else if (dbProductName.contains("postgresql") || dbProductName.contains("postgres")) {
            return "pg_".equalsIgnoreCase(schema);
        } else if (dbProductName.contains("oracle")) {
            return tableName.startsWith("SYS_") || 
                   "SYSTEM".equalsIgnoreCase(schema) || 
                   "SYS".equalsIgnoreCase(schema);
        } else if (dbProductName.contains("h2")) {
            // H2数据库的系统表通常在INFORMATION_SCHEMA或有特定前缀
            return tableName.startsWith("SYSTEM_") ||
                   tableName.startsWith("INFORMATION_SCHEMA") ||
                   "INFORMATION_SCHEMA".equalsIgnoreCase(schema) ||
                   tableName.equals("CATALOGS") ||
                   tableName.equals("COLLATIONS") ||
                   tableName.equals("COLUMNS") ||
                   tableName.equals("COLUMN_PRIVILEGES") ||
                   tableName.equals("CONSTANTS") ||
                   tableName.equals("CONSTRAINTS") ||
                   tableName.equals("CROSS_REFERENCES") ||
                   tableName.equals("DOMAINS") ||
                   tableName.equals("FUNCTION_ALIASES") ||
                   tableName.equals("FUNCTION_COLUMNS") ||
                   tableName.equals("HELP") ||
                   tableName.equals("INDEXES") ||
                   tableName.equals("IN_DOUBT") ||
                   tableName.equals("KEY_COLUMN_USAGE") ||
                   tableName.equals("LOCKS") ||
                   tableName.equals("QUERY_STATISTICS") ||
                   tableName.equals("REFERENTIAL_CONSTRAINTS") ||
                   tableName.equals("RIGHTS") ||
                   tableName.equals("ROLES") ||
                   tableName.equals("SCHEMATA") ||
                   tableName.equals("SEQUENCES") ||
                   tableName.equals("SESSIONS") ||
                   tableName.equals("SESSION_STATE") ||
                   tableName.equals("SETTINGS") ||
                   tableName.equals("SYNONYMS") ||
                   tableName.equals("TABLES") ||
                   tableName.equals("TABLE_CONSTRAINTS") ||
                   tableName.equals("TABLE_PRIVILEGES") ||
                   tableName.equals("TABLE_TYPES") ||
                   tableName.equals("TRIGGERS") ||
                   tableName.equals("TYPE_INFO") ||
                   tableName.equals("VIEWS");
        }
        
        // 通用规则
        return tableName.startsWith("SYS_") || 
               tableName.startsWith("sys_") || 
               "SYSTEM".equalsIgnoreCase(schema);
    }
    
    /**
     * 检查表名是否在明确包含列表中
     */
    private boolean isInIncludedTables(String tableName, String schema) {
        if (includeTables == null || includeTables.trim().isEmpty()) {
            return false;
        }
        
        // 如果是INFORMATION_SCHEMA中的表，默认不包含，除非明确指定schema
        if ("INFORMATION_SCHEMA".equalsIgnoreCase(schema)) {
            // 查找格式为"表名@SCHEMA"的项
            String fullName = tableName + "@" + schema;
            String[] tables = includeTables.split(",");
            for (String table : tables) {
                if (table.trim().equalsIgnoreCase(fullName)) {
                    return true;
                }
            }
            return false;
        }
        
        // 对于非系统schema，检查表名是否在包含列表中
        String[] tables = includeTables.split(",");
        for (String table : tables) {
            table = table.trim();
            // 如果配置项包含@，则需要匹配完整的"表名@SCHEMA"
            if (table.contains("@")) {
                String[] parts = table.split("@", 2);
                if (parts[0].equalsIgnoreCase(tableName) && parts[1].equalsIgnoreCase(schema)) {
                    return true;
                }
            } else if (table.equalsIgnoreCase(tableName)) {
                // 如果配置项不包含@，则只匹配表名
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 检查schema是否在排除列表中
     */
    private boolean isInExcludedSchemas(String schema) {
        if (excludeSchemas == null || excludeSchemas.trim().isEmpty()) {
            return false;
        }
        
        String[] schemas = excludeSchemas.split(",");
        for (String excludedSchema : schemas) {
            if (excludedSchema.trim().equalsIgnoreCase(schema)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 检查表名是否以排除的前缀开头
     */
    private boolean hasExcludedPrefix(String tableName) {
        if (excludeTablePrefixes == null || excludeTablePrefixes.trim().isEmpty()) {
            return false;
        }
        
        String[] prefixes = excludeTablePrefixes.split(",");
        for (String prefix : prefixes) {
            if (tableName.startsWith(prefix.trim())) {
                return true;
            }
        }
        
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
            log.warn("查询表[{}]记录数时出错: {}", tableName, e.getMessage());
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
            log.error("获取表[{}]在{}中的记录数时出错: {}", tableName, dataSourceName, e.getMessage());
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
                    log.warn("获取表 {}.{} 的金额字段时出错: {}", table.getSchema(), table.getTableName(), e.getMessage());
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
            log.warn("获取表[{}]的金额字段时出错: {}", tableName, e.getMessage());
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
        
        // 从三个数据源获取表信息并将其添加到tableInfoMap
        List<TableInfo> primaryTables = getTablesInfoFromDataSource(primaryJdbcTemplate, "primary");
        log.info("从primary数据源获取到{}个表", primaryTables.size());
        addTableInfoToMap(tableInfoMap, primaryTables, "第一数据源", primaryJdbcTemplate);
        
        List<TableInfo> secondaryTables = getTablesInfoFromDataSource(secondaryJdbcTemplate, "secondary");
        log.info("从secondary数据源获取到{}个表", secondaryTables.size());
        addTableInfoToMap(tableInfoMap, secondaryTables, "第二数据源", secondaryJdbcTemplate);
        
        List<TableInfo> tertiaryTables = getTablesInfoFromDataSource(tertiaryJdbcTemplate, "tertiary");
        log.info("从tertiary数据源获取到{}个表", tertiaryTables.size());
        addTableInfoToMap(tableInfoMap, tertiaryTables, "第三数据源", tertiaryJdbcTemplate);
        
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
        for (String key : sortedKeys) {
            String[] parts = key.split("#@#", 2);
            String tableName = parts[0];
            
            TableMetaInfo metaInfo = tableInfoMap.get(key);
            
            // 对于每个表，如果有金额字段，则为每个金额字段创建一个MoneyFieldSumInfo
            if (!metaInfo.getMoneyFields().isEmpty()) {
                List<String> sortedMoneyFields = new ArrayList<>(metaInfo.getMoneyFields());
                Collections.sort(sortedMoneyFields);
                
                // 存储各数据源的批量SUM查询结果
                Map<String, Map<String, BigDecimal>> datasourceSums = new HashMap<>();
                
                // 对三个数据源分别进行批量SUM查询
                if (metaInfo.getDataSources().contains("第一数据源")) {
                    Map<String, BigDecimal> sums = calculateSumBatch(primaryJdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                    datasourceSums.put("第一数据源", sums);
                }
                
                if (metaInfo.getDataSources().contains("第二数据源")) {
                    Map<String, BigDecimal> sums = calculateSumBatch(secondaryJdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                    datasourceSums.put("第二数据源", sums);
                }
                
                if (metaInfo.getDataSources().contains("第三数据源")) {
                    Map<String, BigDecimal> sums = calculateSumBatch(tertiaryJdbcTemplate, metaInfo.getSchema(), tableName, sortedMoneyFields);
                    datasourceSums.put("第三数据源", sums);
                }
                
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
                    
                    // 设置各数据源的SUM值
                    if (metaInfo.getDataSources().contains("第一数据源")) {
                        BigDecimal sum = datasourceSums.get("第一数据源").get(moneyField);
                        sumInfo.setSumValue(1, sum);
                    }
                    
                    if (metaInfo.getDataSources().contains("第二数据源")) {
                        BigDecimal sum = datasourceSums.get("第二数据源").get(moneyField);
                        sumInfo.setSumValue(2, sum);
                    }
                    
                    if (metaInfo.getDataSources().contains("第三数据源")) {
                        BigDecimal sum = datasourceSums.get("第三数据源").get(moneyField);
                        sumInfo.setSumValue(3, sum);
                    }
                    
                    expandedSumInfoList.add(sumInfo);
                }
            } else {
                // 如果表没有金额字段，创建一个空的记录
                MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                    tableName,
                    metaInfo.getSchema(),
                    String.join(" | ", metaInfo.getDataSources()),
                    metaInfo.getFormattedRecordCounts(),
                    "",
                    ""
                );
                expandedSumInfoList.add(sumInfo);
            }
        }
        
        log.info("金额字段SUM值计算完成，共{}条记录", expandedSumInfoList.size());
        
        // 导出到Excel
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
            log.warn("批量计算表 {}.{} 的SUM值时出错: {}", schema, tableName, e.getMessage());
        }
        return results;
    }

    /**
     * 导出动态行列Excel
     */
    private void exportDynamicExcel(String outputPath, List<MoneyFieldSumInfo> dataList) throws IOException {
        if (dataList.isEmpty()) {
            log.warn("没有数据可导出");
            return;
        }
        
        // 创建工作簿和工作表
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("金额字段SUM比对");
            
            // 创建标题行样式
            CellStyle headerStyle = workbook.createCellStyle();
            headerStyle.setAlignment(HorizontalAlignment.CENTER);
            Font headerFont = workbook.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);
            
            // 创建数字单元格样式（金额格式）
            CellStyle numberStyle = workbook.createCellStyle();
            numberStyle.setDataFormat(workbook.createDataFormat().getFormat("#,##0.00"));
            
            // 创建行
            Row headerRow = sheet.createRow(0);
            
            // 固定列的标题
            String[] fixedHeaders = {"表名", "SCHEMA", "所在库", "COUNT", "金额字段", "SUM字段", "SUM1", "SUM2", "SUM3"};
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
                row.createCell(3).setCellValue(info.getRecordCounts());
                row.createCell(4).setCellValue(info.getMoneyFields());
                row.createCell(5).setCellValue(info.getSumField());
                
                // 设置SUM值（使用金额格式）
                for (int i = 1; i <= 3; i++) {
                    BigDecimal sumValue = info.getSumValue(i);
                    if (sumValue != null) {
                        Cell cell = row.createCell(5 + i);
                        cell.setCellValue(sumValue.doubleValue());
                        cell.setCellStyle(numberStyle);
                    } else {
                        row.createCell(5 + i).setCellValue("");
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
} 