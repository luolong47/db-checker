package io.github.luolong47.dbchecker.service;

import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import io.github.luolong47.dbchecker.manager.ThreadPoolManager;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * 表元数据服务，负责从数据源获取表的元数据信息，包括表结构、记录数和金额字段的统计数据。
 * <p>
 * 该服务主要功能包括：
 * <ul>
 *   <li>获取数据源中的所有表信息</li>
 *   <li>识别和处理表中的金额字段</li>
 *   <li>统计表的记录数（含条件和无条件）</li>
 *   <li>计算金额字段的求和（含条件和无条件）</li>
 * </ul>
 * <p>
 * 服务采用多线程并发处理以提高性能，同时支持主从数据源切换和断点续跑。
 *
 * @author luolong47
 */
@Slf4j
@Service
public class TableMetadataService {

    /**
     * 每个数据库最大并发线程数，用于控制单个数据库内的表查询并发度
     */
    private static final int MAX_THREADS_PER_DB = 10;

    /**
     * 支持的数值类型集合，用于识别金额字段
     */
    private static final Set<Integer> NUMERIC_TYPES = new HashSet<>(Arrays.asList(
        Types.NUMERIC, Types.DECIMAL, Types.DOUBLE, Types.FLOAT,
        Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT
    ));

    /** 缓存schema和table的包含关系，避免重复判断 */
    private final Map<String, Boolean> schemaTableCache = new ConcurrentHashMap<>();

    /** Oracle主库数据源 */
    private final JdbcTemplate oraJdbcTemplate;

    /** Oracle从库数据源 */
    private final JdbcTemplate oraSlaveJdbcTemplate;

    /**
     * 线程池管理器
     */
    private final ThreadPoolManager threadPoolManager;

    /**
     * 构造函数，初始化Oracle主从数据源和线程池管理器
     *
     * @param oraJdbcTemplate      Oracle主库数据源
     * @param oraSlaveJdbcTemplate Oracle从库数据源
     * @param threadPoolManager    线程池管理器
     */
    public TableMetadataService(
        @Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate,
        @Qualifier("oraSlaveJdbcTemplate") JdbcTemplate oraSlaveJdbcTemplate,
        ThreadPoolManager threadPoolManager) {
        this.oraJdbcTemplate = oraJdbcTemplate;
        this.oraSlaveJdbcTemplate = oraSlaveJdbcTemplate;
        this.threadPoolManager = threadPoolManager;
    }

    /**
     * 从数据源获取表信息，同时获取表的记录数和金额字段的求和。
     * <p>
     * 该方法会：
     * <ul>
     *   <li>检查数据库是否需要处理（支持断点续跑）</li>
     *   <li>获取所有符合条件的表</li>
     *   <li>并发处理每个表的元数据信息</li>
     *   <li>统计表的记录数和金额字段求和</li>
     * </ul>
     *
     * @param jdbcTemplate JdbcTemplate实例，用于数据库操作
     * @param dataSourceName 数据源名称
     * @param includeSchemas 需要包含的schema列表，多个schema用逗号分隔
     * @param includeTables 需要包含的表名列表，多个表名用逗号分隔
     * @param whereConditionConfig 条件配置，用于过滤记录
     * @param resumeStateManager 断点续跑状态管理器
     * @param runMode 运行模式
     * @return 表信息列表，如果出错则返回空列表
     */
    public List<TableInfo> getTablesInfoFromDataSource(JdbcTemplate jdbcTemplate, String dataSourceName, String includeSchemas, String includeTables, DbConfig whereConditionConfig, ResumeStateManager resumeStateManager, String runMode) {
        // 检查是否应该处理该数据库
        if (!resumeStateManager.shouldProcessDatabase(dataSourceName, runMode, "")) {
            log.info("数据源[{}]已处理或不需要处理，跳过", dataSourceName);
            return Collections.emptyList();
        }

        log.info("开始获取数据源[{}]的表信息", dataSourceName);

        List<TableInfo> tables = new ArrayList<>();

        // 使用Optional避免NullPointerException
        return Optional.ofNullable(jdbcTemplate.getDataSource()).map(dataSource -> {
            try (Connection connection = dataSource.getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet tablesResultSet = metaData.getTables(null, null, "%", new String[]{"TABLE"});

                // 收集需要处理的表
                List<String> tableTasks = new ArrayList<>();

                while (tablesResultSet.next()) {
                    String tableName = tablesResultSet.getString("TABLE_NAME");
                    // 数字开头的表名要加引号
                    if (ReUtil.isMatch("^\\d+.*", tableName)) {
                        tableName = StrUtil.format("\"{}\"", tableName);
                    }
                    String tableSchema = tablesResultSet.getString("TABLE_SCHEM");

                    // 判断是否应该排除该表或已处理过该表
                    if (shouldExcludeTable(tableName, tableSchema, includeSchemas, includeTables) || !resumeStateManager.shouldProcessTable(tableName, dataSourceName, runMode, "")) {
                        continue;
                    }

                    // 将需要处理的表添加到任务列表
                    tableTasks.add(tableName);
                }

                tablesResultSet.close();
                log.info("数据源[{}]发现{}个需要处理的表", dataSourceName, tableTasks.size());

                // 使用线程池并发处理表
                if (!tableTasks.isEmpty()) {
                    processTablesInParallel(jdbcTemplate, dataSourceName, metaData, tableTasks, whereConditionConfig, resumeStateManager, tables);
                }
                // 标记该数据库为已处理
                resumeStateManager.markDatabaseProcessed(dataSourceName);
                return tables;
            } catch (SQLException e) {
                log.error("从数据源[{}]获取表信息时出错: {}", dataSourceName, e.getMessage(), e);
                return Collections.<TableInfo>emptyList();
            }
        }).orElseGet(() -> {
            log.error("数据源[{}]的DataSource为null", dataSourceName);
            return Collections.emptyList();
        });
    }

    /**
     * 并发处理多个表的元数据信息。
     * <p>
     * 该方法使用线程池来并发处理表的元数据信息，以提高处理效率。每个表的处理都在独立的线程中进行，
     * 处理完成后的结果会被同步添加到结果列表中。
     *
     * @param jdbcTemplate JdbcTemplate实例，用于数据库操作
     * @param dataSourceName 数据源名称
     * @param metaData 数据库元数据对象
     * @param tableTasks 需要处理的表任务列表
     * @param whereConditionConfig 条件配置，用于过滤记录
     * @param resumeStateManager 断点续跑状态管理器
     * @param resultTables 存储处理结果的列表
     */
    private void processTablesInParallel(JdbcTemplate jdbcTemplate, String dataSourceName, DatabaseMetaData metaData, List<String> tableTasks, DbConfig whereConditionConfig, ResumeStateManager resumeStateManager, List<TableInfo> resultTables) {

        // 获取数据库专用线程池
        ExecutorService executor = threadPoolManager.getDbThreadPool(dataSourceName);
        log.info("使用[{}]数据库的专用线程池处理{}个表", dataSourceName, tableTasks.size());

        // 创建CompletableFuture任务列表
        List<CompletableFuture<TableInfo>> futures = new ArrayList<>();

        for (String task : tableTasks) {
            CompletableFuture<TableInfo> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.debug("开始处理表: {}", task);
                    TableInfo tableInfo = processTable(jdbcTemplate, task, dataSourceName, metaData, whereConditionConfig);

                    if (tableInfo != null) {
                        // 使用synchronized确保线程安全
                        synchronized (resumeStateManager) {
                            // 标记该表为已处理
                            resumeStateManager.markTableProcessed(task, dataSourceName);
                        }
                        return tableInfo;
                    }
                } catch (Exception e) {
                    log.error("处理表[{}]时出错: {}", task, e.getMessage(), e);
                }
                return null;
            }, executor);

            futures.add(future);
        }

        // 等待所有任务完成并收集结果
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        for (CompletableFuture<TableInfo> future : futures) {
            try {
                TableInfo tableInfo = future.get();
                if (tableInfo != null) {
                    synchronized (resultTables) {
                        resultTables.add(tableInfo);
                    }
                }
            } catch (Exception e) {
                log.error("获取表处理结果时出错: {}", e.getMessage(), e);
            }
        }

        // 不关闭线程池，现在由ThreadPoolManager管理
    }

    /**
     * 处理单个表，获取其字段信息、记录数和金额字段SUM值。
     * <p>
     * 该方法会：
     * <ul>
     *   <li>创建表信息对象</li>
     *   <li>识别并收集金额字段</li>
     *   <li>统计记录数和金额字段的求和</li>
     * </ul>
     *
     * @param jdbcTemplate JdbcTemplate实例，用于数据库操作
     * @param tableName 表名
     * @param dataSourceName 数据源名称
     * @param metaData 数据库元数据对象
     * @param whereConditionConfig 条件配置，用于过滤记录
     * @return 表信息对象，如果处理失败则返回null
     * @throws SQLException 如果数据库操作出错
     */
    private TableInfo processTable(JdbcTemplate jdbcTemplate, String tableName, String dataSourceName, DatabaseMetaData metaData, DbConfig whereConditionConfig) throws SQLException {
        TableInfo tableInfo = new TableInfo(tableName);
        tableInfo.setDataSourceName(dataSourceName);

        // 获取金额字段并添加到TableInfo中
        findMoneyFields(tableName, metaData, tableInfo);

        // 获取表记录数和金额字段SUM值
        if (!fetchRecordCountAndSums(jdbcTemplate, tableName, dataSourceName, tableInfo, whereConditionConfig)) {
            return null;
        }

        return tableInfo;
    }

    /**
     * 查找表中的金额字段。
     * <p>
     * 该方法通过分析表的列元数据来识别金额字段，判断标准为：
     * <ul>
     *   <li>字段类型为数值型（如NUMERIC, DECIMAL等）</li>
     *   <li>字段的小数位数大于0</li>
     * </ul>
     * 识别到的金额字段会被添加到TableInfo对象中。
     *
     * @param tableName 表名
     * @param metaData 数据库元数据对象
     * @param tableInfo 表信息对象，用于存储识别到的金额字段
     * @throws SQLException 如果获取列元数据时发生错误
     */
    private void findMoneyFields(String tableName, DatabaseMetaData metaData, TableInfo tableInfo) throws SQLException {
        try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                int dataType = columns.getInt("DATA_TYPE");
                int decimalDigits = columns.getInt("DECIMAL_DIGITS");

                // 判断是否为金额字段：数值型且小数位不为0
                if (isNumericType(dataType) && decimalDigits > 0) {
                    tableInfo.addMoneyField(columnName);
                    log.debug("发现金额字段: {}.{}, 类型: {}, 小数位: {}", tableName, columnName, dataType, decimalDigits);
                }
            }
        }

        if (!tableInfo.getMoneyFields().isEmpty()) {
            log.debug("表[{}]中发现{}个金额字段: {}", tableName, tableInfo.getMoneyFields().size(), StrUtil.join(", ", tableInfo.getMoneyFields()));
        }
    }

    /**
     * 获取表的记录数和金额字段SUM值。
     * <p>
     * 该方法会：
     * <ul>
     *   <li>根据配置选择合适的数据源（主库/从库）</li>
     *   <li>构建优化的SQL查询，同时获取有条件和无条件的统计数据</li>
     *   <li>计算表的总记录数和满足条件的记录数</li>
     *   <li>计算所有金额字段的求和（含条件和无条件）</li>
     * </ul>
     *
     * @param jdbcTemplate JdbcTemplate实例，用于数据库操作
     * @param tableName 表名
     * @param dataSourceName 数据源名称
     * @param tableInfo 表信息对象，用于存储统计结果
     * @param whereConditionConfig 条件配置，用于过滤记录
     * @return 是否成功获取统计数据
     */
    private boolean fetchRecordCountAndSums(JdbcTemplate jdbcTemplate, String tableName, String dataSourceName, TableInfo tableInfo, DbConfig whereConditionConfig) {
        try {
            // 判断是否应该使用从节点查询
            String dataSourceToUse = whereConditionConfig.getDataSourceToUse(tableName, dataSourceName);

            // 根据数据源名称选择正确的JdbcTemplate
            JdbcTemplate templateToUse = jdbcTemplate;
            if ("ora-slave".equals(dataSourceToUse) && "ora".equals(dataSourceName)) {
                templateToUse = oraSlaveJdbcTemplate;
            }

            // 使用不同的dataSourceName来构建日志信息，但不修改TableInfo中的dataSourceName
            String logDataSourceName = dataSourceName;
            if (!dataSourceToUse.equals(dataSourceName)) {
                logDataSourceName = dataSourceToUse + "(替代" + dataSourceName + ")";
            }

            // 获取所有金额字段
            List<String> moneyFields = new ArrayList<>(tableInfo.getMoneyFields());

            // 获取WHERE条件
            String whereCondition = whereConditionConfig.getCondition(dataSourceName, tableName);
            boolean hasWhereCondition = whereCondition != null && !whereCondition.isEmpty();

            // 构建优化后的SQL查询，同时获取有条件和无条件的统计数据
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("SELECT ");

            // 计算总记录数
            sqlBuilder.append("COUNT(*) AS \"_record_count_all\"");

            // 如果有WHERE条件，计算满足条件的记录数
            if (hasWhereCondition) {
                sqlBuilder.append(", SUM(CASE WHEN ").append(whereCondition).append(" THEN 1 ELSE 0 END) AS \"_record_count\"");
            } else {
                sqlBuilder.append(", COUNT(*) AS \"_record_count\"");
            }

            // 添加金额字段的聚合
            for (String field : moneyFields) {
                // 无条件SUM
                sqlBuilder.append(", SUM(").append(field).append(") AS \"").append(field).append("_ALL\"");

                // 有条件SUM
                if (hasWhereCondition) {
                    sqlBuilder.append(", SUM(CASE WHEN ").append(whereCondition).append(" THEN ").append(field).append(" ELSE 0 END) AS \"").append(field).append("\"");
                } else {
                    sqlBuilder.append(", SUM(").append(field).append(") AS \"").append(field).append("\"");
                }
            }

            // 添加表名
            sqlBuilder.append(" FROM ").append(tableName);

            // 应用SQL提示
            String sql = whereConditionConfig.applySqlHint(sqlBuilder.toString(), dataSourceName, tableName);

            log.debug("执行优化的COUNT和SUM查询[{}]: {}", logDataSourceName, sql);
            
            try {
                // 执行查询并获取结果
                Map<String, Object> resultMap = templateToUse.queryForMap(sql);

                // 提取总记录数（无条件）
                Object recordCountAllObj = resultMap.get("_record_count_all");
                int recordCountAll = 0;

                if (recordCountAllObj != null) {
                    if (recordCountAllObj instanceof Number) {
                        recordCountAll = ((Number) recordCountAllObj).intValue();
                    } else {
                        try {
                            recordCountAll = Integer.parseInt(recordCountAllObj.toString());
                        } catch (NumberFormatException e) {
                            log.error("无法将 {} 转换为整数: {}", recordCountAllObj, e.getMessage());
                        }
                    }
                }

                // 设置无WHERE条件的记录数
                tableInfo.setRecordCountAll(dataSourceName, recordCountAll);
                log.debug("表[{}]在数据源[{}]中的无条件记录数: {}", tableName, logDataSourceName, recordCountAll);

                // 提取有条件记录数
                Object recordCountObj = resultMap.get("_record_count");
                int recordCount = 0;

                if (recordCountObj != null) {
                    if (recordCountObj instanceof Number) {
                        recordCount = ((Number) recordCountObj).intValue();
                    } else {
                        try {
                            recordCount = Integer.parseInt(recordCountObj.toString());
                        } catch (NumberFormatException e) {
                            log.error("无法将 {} 转换为整数: {}", recordCountObj, e.getMessage());
                        }
                    }
                }

                // 设置有WHERE条件的记录数
                tableInfo.setRecordCount(dataSourceName, recordCount);
                log.debug("表[{}]在数据源[{}]中的记录数: {}", tableName, logDataSourceName, recordCount);

                // 处理金额字段SUM结果
                if (recordCountAll > 0 && !moneyFields.isEmpty()) {
                    // 处理所有金额字段
                    for (String fieldName : moneyFields) {
                        // 设置有条件SUM值
                        Object value = resultMap.get(fieldName);
                        BigDecimal sumValue = null;
                        if (value != null) {
                            try {
                                if (value instanceof BigDecimal) {
                                    sumValue = (BigDecimal) value;
                                } else if (value instanceof Number) {
                                    sumValue = BigDecimal.valueOf(((Number) value).doubleValue());
                                } else {
                                    sumValue = new BigDecimal(value.toString());
                                }
                            } catch (NumberFormatException e) {
                                log.error("无法将 {} 转换为 BigDecimal: {}", value, e.getMessage());
                            }
                        }
                        tableInfo.setMoneySum(dataSourceName, fieldName, sumValue);

                        // 设置无条件SUM值
                        String fieldNameAll = fieldName + "_ALL";
                        value = resultMap.get(fieldNameAll);
                        BigDecimal sumValueAll = null;
                        if (value != null) {
                            try {
                                if (value instanceof BigDecimal) {
                                    sumValueAll = (BigDecimal) value;
                                } else if (value instanceof Number) {
                                    sumValueAll = BigDecimal.valueOf(((Number) value).doubleValue());
                                } else {
                                    sumValueAll = new BigDecimal(value.toString());
                                }
                            } catch (NumberFormatException e) {
                                log.error("无法将 {} 转换为 BigDecimal: {}", value, e.getMessage());
                            }
                        }
                        tableInfo.setMoneySumAll(dataSourceName, fieldName, sumValueAll);
                        log.debug("设置表[{}]字段[{}]在数据源[{}]中的无条件SUM值为: {}", tableName, fieldName, dataSourceName, sumValueAll);
                        log.debug("表 {} 字段 {} 的SUM值为: {}", tableName, fieldName, sumValue);
                    }

                    log.debug("当前表[{}]的moneySumsAll内容: {}", tableName, tableInfo.getAllMoneySumsAll());
                }

                return true;
            } catch (Exception e) {
                log.error("执行优化的COUNT和SUM查询出错[{}]：{}", tableName, e.getMessage());
                return false;
            }
        } catch (Exception e) {
            log.error("处理表 {} 的记录数和金额字段SUM时出错: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 判断表是否应该被排除。
     * <p>
     * 该方法根据配置的包含规则来判断表是否需要处理：
     * <ul>
     *   <li>如果指定了schema列表，检查表的schema是否在列表中</li>
     *   <li>如果指定了表名列表，检查表名是否在列表中</li>
     * </ul>
     * 判断结果会被缓存以提高性能。
     *
     * @param tableName 表名
     * @param tableSchema 表所属的schema
     * @param includeSchemas 需要包含的schema列表，多个schema用逗号分隔
     * @param includeTables 需要包含的表名列表，多个表名用逗号分隔
     * @return 如果表应该被排除则返回true，否则返回false
     */
    private boolean shouldExcludeTable(String tableName, String tableSchema, String includeSchemas, String includeTables) {
        String cacheKey = tableSchema + ":" + tableName + ":" + includeSchemas + ":" + includeTables;
        return schemaTableCache.computeIfAbsent(cacheKey, k -> {
            // 如果指定了schema，检查当前表的schema是否在列表中
            if (StrUtil.isNotEmpty(includeSchemas)) {
                List<String> schemaList = Arrays.asList(includeSchemas.split(","));
                if (!schemaList.contains(tableSchema)) {
                    return true;
                }
            }

            // 如果指定了表名，检查当前表是否在列表中
            if (StrUtil.isNotEmpty(includeTables)) {
                List<String> tableList = Arrays.asList(includeTables.split(","));
                String tableNameWithSchema = StrUtil.format("{}@{}", tableName, tableSchema);
                return !tableList.contains(tableName) && !tableList.contains(tableNameWithSchema);
            }

            return false;
        });
    }

    /**
     * 判断数据类型是否为数值型
     *
     * @param sqlType java.sql.Types中的类型码
     * @return 是否为数值型
     */
    private boolean isNumericType(int sqlType) {
        return NUMERIC_TYPES.contains(sqlType);
    }

    /**
     * 检查指定表是否应该在特定数据库中处理
     *
     * @param dataSourceName 数据源名称
     * @param tableName      表名
     * @param dbConfig       数据库配置
     * @return 如果该表应该在这个数据库中处理，则返回true
     */
    public boolean shouldProcessTable(String dataSourceName, String tableName, DbConfig dbConfig) {
        // 检查表名是否符合配置的包含规则
        if (dbConfig.getIncludeTables() != null && !dbConfig.getIncludeTables().isEmpty()) {
            boolean matched = false;
            for (String pattern : dbConfig.getIncludeTables().split(",")) {
                if (pattern.trim().equalsIgnoreCase(tableName) ||
                    (pattern.contains("*") && matches(tableName, pattern.trim()))) {
                    matched = true;
                    break;
                }
            }
            if (!matched) {
                return false;
            }
        }

        // 获取表可能应该存在的数据源列表
        Set<String> possibleDataSources = getPossibleDataSourcesForTable(tableName);

        // 如果当前数据源在可能的数据源列表中，则表应该被处理
        return possibleDataSources.contains(dataSourceName);
    }

    /**
     * 获取表可能存在的数据源列表
     *
     * @param tableName 表名
     * @return 可能包含该表的数据源集合
     */
    private Set<String> getPossibleDataSourcesForTable(String tableName) {
        // 这里的逻辑基于表命名规则和业务逻辑来判断表应该出现在哪些数据库中
        // 例如，所有表都应该出现在ora中，但只有特定表会出现在其他数据库
        Set<String> dataSources = new HashSet<>();

        // 默认所有表都应该在ora中
        dataSources.add("ora");

        // 基于表名前缀或特定规则添加其他可能的数据源
        if (tableName.startsWith("RLCMS_")) {
            dataSources.add("rlcms_base");
            dataSources.add("rlcms_pv1");
            dataSources.add("rlcms_pv2");
            dataSources.add("rlcms_pv3");
        } else if (tableName.startsWith("BS_")) {
            dataSources.add("bscopy_pv1");
            dataSources.add("bscopy_pv2");
            dataSources.add("bscopy_pv3");
        } else {
            // 对于其他不确定的表，添加所有数据源以确保不漏处理
            dataSources.add("rlcms_base");
            dataSources.add("rlcms_pv1");
            dataSources.add("rlcms_pv2");
            dataSources.add("rlcms_pv3");
            dataSources.add("bscopy_pv1");
            dataSources.add("bscopy_pv2");
            dataSources.add("bscopy_pv3");
        }

        return dataSources;
    }

    /**
     * 检查字符串是否匹配通配符模式
     *
     * @param str     要检查的字符串
     * @param pattern 通配符模式，可以包含*
     * @return 是否匹配
     */
    private boolean matches(String str, String pattern) {
        // 将通配符模式转换为正则表达式
        String regex = pattern.replace(".", "\\.").replace("*", ".*");
        return str.matches(regex);
    }
}