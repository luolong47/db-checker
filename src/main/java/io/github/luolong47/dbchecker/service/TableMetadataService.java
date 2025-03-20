package io.github.luolong47.dbchecker.service;

import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
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
import java.util.concurrent.Executors;

/**
 * 表元数据服务
 */
@Slf4j
@Service
public class TableMetadataService {

    // 创建一个线程池，用于处理单个数据库内的表查询
    private static final int MAX_THREADS_PER_DB = 10; // 每个数据库最大并发线程数

    // 缓存数值类型
    private static final Set<Integer> NUMERIC_TYPES = new HashSet<>(Arrays.asList(Types.NUMERIC, Types.DECIMAL, Types.DOUBLE, Types.FLOAT, Types.INTEGER, Types.BIGINT, Types.SMALLINT, Types.TINYINT));
    // 缓存schema和table的包含关系
    private final Map<String, Boolean> schemaTableCache = new ConcurrentHashMap<>();

    private final JdbcTemplate oraJdbcTemplate;
    private final JdbcTemplate oraSlaveJdbcTemplate;

    public TableMetadataService(@Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate, @Qualifier("oraSlaveJdbcTemplate") JdbcTemplate oraSlaveJdbcTemplate) {
        this.oraJdbcTemplate = oraJdbcTemplate;
        // 保存所有JdbcTemplate的引用
        this.oraSlaveJdbcTemplate = oraSlaveJdbcTemplate;
    }

    /**
     * 从数据源获取表信息，同时获取表的记录数和金额字段的求和
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
                List<TableTask> tableTasks = new ArrayList<>();

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
                    tableTasks.add(new TableTask(tableName));
                }

                tablesResultSet.close();
                log.info("数据源[{}]发现{}个需要处理的表", dataSourceName, tableTasks.size());

                // 使用线程池并发处理表
                if (!tableTasks.isEmpty()) {
                    processTablesInParallel(jdbcTemplate, dataSourceName, metaData, tableTasks, whereConditionConfig, resumeStateManager, tables);
                }

                log.info("从{}获取到{}个非系统表", dataSourceName, tables.size());

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
     * 并发处理多个表
     */
    private void processTablesInParallel(JdbcTemplate jdbcTemplate, String dataSourceName, DatabaseMetaData metaData, List<TableTask> tableTasks, DbConfig whereConditionConfig, ResumeStateManager resumeStateManager, List<TableInfo> resultTables) {

        // 创建线程池，控制并发数量
        ExecutorService executor = Executors.newFixedThreadPool(Math.min(MAX_THREADS_PER_DB, tableTasks.size()));

        // 创建CompletableFuture任务列表
        List<CompletableFuture<TableInfo>> futures = new ArrayList<>();

        for (TableTask task : tableTasks) {
            CompletableFuture<TableInfo> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.debug("开始处理表: {}", task.tableName);
                    TableInfo tableInfo = processTable(jdbcTemplate, task.tableName, dataSourceName, metaData, whereConditionConfig);

                    if (tableInfo != null) {
                        // 使用synchronized确保线程安全
                        synchronized (resumeStateManager) {
                            // 标记该表为已处理
                            resumeStateManager.markTableProcessed(task.tableName, dataSourceName);
                        }
                        return tableInfo;
                    }
                } catch (Exception e) {
                    log.error("处理表[{}]时出错: {}", task.tableName, e.getMessage(), e);
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

        // 关闭线程池
        executor.shutdown();
    }

    /**
     * 处理单个表，获取其字段信息、记录数和金额字段SUM值
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
     * 查找表中的金额字段
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
            log.info("表[{}]中发现{}个金额字段: {}", tableName, tableInfo.getMoneyFields().size(), StrUtil.join(", ", tableInfo.getMoneyFields()));
        }
    }

    /**
     * 获取表的记录数和金额字段SUM值
     *
     * @return 是否成功获取
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

            // 构建SQL查询语句 - 无论是否有金额字段都使用同一种查询方式
            StringBuilder sqlBuilder = new StringBuilder("SELECT COUNT(*) AS \"_record_count\"");

            // 如果有金额字段，添加SUM表达式
            if (!moneyFields.isEmpty()) {
                for (String field : moneyFields) {
                    sqlBuilder.append(", SUM(").append(field).append(") AS \"").append(field).append("\"");
                }
            }

            // 添加表名
            sqlBuilder.append(" FROM ").append(tableName);

            // 生成最终SQL
            String sql = sqlBuilder.toString();

            // 应用WHERE条件
            sql = whereConditionConfig.applyCondition(sql, dataSourceName, tableName);

            // 应用SQL提示
            sql = whereConditionConfig.applySqlHint(sql, dataSourceName, tableName);

            // 根据是否有金额字段选择日志级别和消息
            if (!moneyFields.isEmpty()) {
                log.info("执行合并的COUNT和SUM查询[{}]: {}", logDataSourceName, sql);
            } else {
                log.debug("执行COUNT查询[{}]: {}", logDataSourceName, sql);
            }

            try {
                // 执行查询并获取结果
                Map<String, Object> resultMap = templateToUse.queryForMap(sql);

                // 提取记录数
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

                // 设置记录数
                tableInfo.setRecordCount(dataSourceName, recordCount);
                log.debug("表[{}]在数据源[{}]中的记录数: {}", tableName, logDataSourceName, recordCount);

                // 如果有金额字段且记录数大于0，处理SUM结果
                if (!moneyFields.isEmpty() && recordCount > 0) {
                    moneyFields.forEach(fieldName -> {
                        Optional<BigDecimal> decimalOptional = Optional.ofNullable(resultMap.get(fieldName))
                            .map(value -> {
                                if (value instanceof BigDecimal) {
                                    return (BigDecimal) value;
                                } else {
                                    try {
                                        return new BigDecimal(value.toString());
                                    } catch (NumberFormatException e) {
                                        log.error("无法将 {} 转换为 BigDecimal: {}", value, e.getMessage());
                                        return null;
                                    }
                                }
                            });

                        // 设置SUM值
                        BigDecimal sumValue = decimalOptional.orElse(BigDecimal.ZERO);
                        tableInfo.setMoneySum(dataSourceName, fieldName, sumValue);
                        log.debug("表 {} 字段 {} 的SUM值为: {}", tableName, fieldName, sumValue);
                    });
                } else if (!moneyFields.isEmpty()) {
                    // 记录数为0时，所有金额字段SUM都设为0
                    moneyFields.forEach(fieldName ->
                        tableInfo.setMoneySum(dataSourceName, fieldName, BigDecimal.ZERO));
                }

                return true;
            } catch (Exception e) {
                // 统一处理所有异常
                log.error("执行表[{}]的查询时出错: {}", tableName, e.getMessage());
                return false;
            }
        } catch (Exception e) {
            log.error("处理表[{}]时出错: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 判断表是否应该被排除
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
     * 表任务类，用于存储需要处理的表信息
     */
    private static class TableTask {
        private final String tableName;

        public TableTask(String tableName) {
            this.tableName = tableName;
        }
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
} 