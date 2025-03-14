package io.github.luolong47.dbchecker.service;

import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 表元数据服务
 */
@Slf4j
@Service
public class TableMetadataService {

    /**
     * 从数据源获取表信息，同时获取表的记录数和金额字段的求和
     */
    public List<TableInfo> getTablesInfoFromDataSource(JdbcTemplate jdbcTemplate, String dataSourceName,
                                                       String includeSchemas, String includeTables,
                                                       DbConfig whereConditionConfig,
                                                       ResumeStateManager resumeStateManager, String runMode) {
        // 检查是否应该处理该数据库
        if (!resumeStateManager.shouldProcessDatabase(dataSourceName, runMode, "")) {
            log.info("数据源[{}]已处理或不需要处理，跳过", dataSourceName);
            return Collections.emptyList();
        }

        log.info("开始获取数据源[{}]的表信息", dataSourceName);

        List<TableInfo> tables = new ArrayList<>();

        // 使用Optional避免NullPointerException
        Optional.ofNullable(jdbcTemplate.getDataSource())
            .map(dataSource -> {
                try (Connection connection = dataSource.getConnection()) {
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
                        if (shouldExcludeTable(tableName, tableSchema, includeSchemas, includeTables) ||
                            !resumeStateManager.shouldProcessTable(tableName, dataSourceName, runMode, "")) {
                            continue;
                        }

                        // 处理单个表
                        try {
                            TableInfo tableInfo = processTable(jdbcTemplate, tableName, dataSourceName, metaData, whereConditionConfig);
                            if (tableInfo != null) {
                                tables.add(tableInfo);
                                // 标记该表为已处理
                                resumeStateManager.markTableProcessed(tableName, dataSourceName);
                            }
                        } catch (Exception e) {
                            log.error("处理表[{}]时出错: {}", tableName, e.getMessage(), e);
                        }
                    }

                    tablesResultSet.close();
                    log.info("从{}获取到{}个非系统表", dataSourceName, tables.size());

                    // 标记该数据库为已处理
                    resumeStateManager.markDatabaseProcessed(dataSourceName);
                    return tables;
                } catch (SQLException e) {
                    log.error("从数据源[{}]获取表信息时出错: {}", dataSourceName, e.getMessage(), e);
                    return Collections.<TableInfo>emptyList();
                }
            })
            .orElseGet(() -> {
                log.error("数据源[{}]的DataSource为null", dataSourceName);
                return Collections.emptyList();
            });

        return tables;
    }

    /**
     * 处理单个表，获取其字段信息、记录数和金额字段SUM值
     */
    private TableInfo processTable(JdbcTemplate jdbcTemplate, String tableName, String dataSourceName,
                                   DatabaseMetaData metaData, DbConfig whereConditionConfig) throws SQLException {
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
                    log.debug("发现金额字段: {}.{}, 类型: {}, 小数位: {}",
                        tableName, columnName, dataType, decimalDigits);
                }
            }
        }

        if (!tableInfo.getMoneyFields().isEmpty()) {
            log.info("表[{}]中发现{}个金额字段: {}",
                tableName, tableInfo.getMoneyFields().size(),
                StrUtil.join(", ", tableInfo.getMoneyFields()));
        }
    }

    /**
     * 获取表的记录数和金额字段SUM值
     *
     * @return 是否成功获取
     */
    private boolean fetchRecordCountAndSums(JdbcTemplate jdbcTemplate, String tableName,
                                            String dataSourceName, TableInfo tableInfo,
                                            DbConfig whereConditionConfig) {
        try {
            // 查询记录数
            String countSql = StrUtil.format("SELECT COUNT(*) FROM {}", tableName);
            // 应用WHERE条件
            countSql = whereConditionConfig.applyCondition(countSql, dataSourceName, tableName);
            log.debug("执行SQL: {}", countSql);

            // 使用Optional处理可能为null的结果，但用Java 8兼容的方式
            Optional<Long> countOptional = Optional.ofNullable(jdbcTemplate.queryForObject(countSql, Long.class));
            if (countOptional.isPresent()) {
                Long count = countOptional.get();
                tableInfo.setRecordCount(dataSourceName, count);
                log.info("表[{}]在{}中有{}条记录", tableName, dataSourceName, count);
            } else {
                tableInfo.setRecordCount(dataSourceName, 0L);
                log.info("表[{}]在{}中有0条记录", tableName, dataSourceName);
            }

            // 如果有金额字段，计算它们的SUM
            if (!tableInfo.getMoneyFields().isEmpty()) {
                fetchMoneySums(jdbcTemplate, tableName, dataSourceName, tableInfo, whereConditionConfig);
            }

            return true;
        } catch (BadSqlGrammarException e) {
            log.error("获取表[{}]的记录数或SUM值时出错: {}", tableName, e.getMessage());
            return false;
        } catch (Exception e) {
            log.error("获取表[{}]的记录数或SUM值时出错: {}", tableName, e.getMessage(), e);
            return false;
        }
    }

    /**
     * 获取金额字段的SUM值
     */
    private void fetchMoneySums(JdbcTemplate jdbcTemplate, String tableName,
                                String dataSourceName, TableInfo tableInfo,
                                DbConfig whereConditionConfig) {
        // 构建查询语句，一次查询所有字段的SUM
        List<String> sumExpressions = tableInfo.getMoneyFields().stream()
            .map(field -> StrUtil.format("SUM({}) AS \"{}\"", field, field))
            .collect(Collectors.toList());

        String sumSql = StrUtil.format("SELECT {} FROM {}",
            StrUtil.join(", ", sumExpressions), tableName);

        // 应用WHERE条件
        sumSql = whereConditionConfig.applyCondition(sumSql, dataSourceName, tableName);
        log.info("执行批量SUM查询: {}", sumSql);

        // 执行查询并映射结果
        try {
            Map<String, Object> resultMap = jdbcTemplate.queryForMap(sumSql);

            // 使用Stream API处理所有金额字段的SUM结果，但用Java 8兼容的方式
            tableInfo.getMoneyFields().forEach(fieldName -> {
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

                if (decimalOptional.isPresent()) {
                    BigDecimal decimalValue = decimalOptional.get();
                    tableInfo.setMoneySum(dataSourceName, fieldName, decimalValue);
                    log.debug("表 {} 字段 {} 的SUM值为: {}", tableName, fieldName, decimalValue);
                } else {
                    tableInfo.setMoneySum(dataSourceName, fieldName, BigDecimal.ZERO);
                }
            });
        } catch (Exception e) {
            log.error("执行SUM查询时出错: {}", e.getMessage(), e);
        }
    }

    /**
     * 判断表是否应该被排除
     */
    private boolean shouldExcludeTable(String tableName, String schema, String includeSchemas, String includeTables) {
        // 检查schema是否在包含列表中
        boolean schemaIncluded = Arrays.stream(includeSchemas.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .anyMatch(s -> s.equals(schema));

        if (!schemaIncluded) {
            return true;
        }

        // 检查表名是否在包含列表中
        return Arrays.stream(includeTables.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.split("@")[0])
            .noneMatch(s -> s.equals(tableName));
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
} 