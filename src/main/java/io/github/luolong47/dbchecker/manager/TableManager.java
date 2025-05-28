package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.StopWatch;
import cn.hutool.core.util.StrUtil;
import cn.hutool.extra.spring.SpringUtil;
import io.github.luolong47.dbchecker.config.Dbconfig;
import io.github.luolong47.dbchecker.entity.*;
import io.github.luolong47.dbchecker.service.TableService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Slf4j
@Data
@Component
@Order(100)
public class TableManager {
    private List<String> tables;
    private List<String> schemas;
    private List<String> dbs = ListUtil.of("ora", "ora-slave", "rlcms-base", "rlcms-pv1", "rlcms-pv2", "rlcms-pv3", "bscopy-pv1", "bscopy-pv2", "bscopy-pv3");
    private Map<String, List<String>> tb2dbs;
    private Map<String, String> tb2Schema = new ConcurrentHashMap<>();
    private Map<String, List<String>> tb2sumCols;
    private Map<String, TableService> tableServices;
    private Map<String, List<TableCsvResult>> tableCsvResultMap = new ConcurrentHashMap<>();
    private Map<String, TableInfo> tableInfoMap;
    private Map<String, Map<String, String>> tb2where; //tableName->(db->whereStr)
    private List<String> slaveQueryTbs = new CopyOnWriteArrayList<>(); // 存储需要从从节点查询的表名列表
    private Map<String, String> tb2hint;
    private Map<String, Formula> tb2formula;
    private final Dbconfig dbconfig;
    private final DynamicJdbcTemplateManager dynamicJdbcTemplateManager;
    private final CsvExportManager csvExportManager;
    private final ResumeStateManager resumeStateManager;
    
    // 注入不同用途的线程池
    private final ExecutorService tableExecutor;
    
    private final ExecutorService dbQueryExecutor;
    
    private final ExecutorService csvExportExecutor;

    // 存储表处理的StopWatch对象
    private Map<String, StopWatch> tableStopWatches = new ConcurrentHashMap<>();
    
    // 存储数据库查询的StopWatch对象：表名-数据库名->StopWatch
    private Map<String, Map<String, StopWatch>> dbQueryStopWatches = new ConcurrentHashMap<>();
    
    // 存储CSV导出的StopWatch对象
    private Map<String, StopWatch> csvExportStopWatches = new ConcurrentHashMap<>();
    
    // 全局表处理的StopWatch对象
    private StopWatch globalTableWatch;

    public TableManager(Dbconfig dbconfig, DynamicJdbcTemplateManager dynamicJdbcTemplateManager, CsvExportManager csvExportManager, ResumeStateManager resumeStateManager, ExecutorService tableExecutor, ExecutorService dbQueryExecutor, ExecutorService csvExportExecutor) {
        this.dbconfig = dbconfig;
        this.dynamicJdbcTemplateManager = dynamicJdbcTemplateManager;
        this.csvExportManager = csvExportManager;
        this.resumeStateManager = resumeStateManager;
        this.tableExecutor = tableExecutor;
        this.dbQueryExecutor = dbQueryExecutor;
        this.csvExportExecutor = csvExportExecutor;
    }

    @PostConstruct
    public void init() {
        log.info("TableManager开始初始化，等待数据库初始化完成...");

        // 此时DatabaseInitService的PostConstruct已经执行完成，因为它的Order值较小

        // 从db.include.tables配置中初始化表列表
        tables = Optional.ofNullable(dbconfig.getInclude().getTables())
            .filter(s -> !s.trim().isEmpty())
            .map(s -> Arrays.stream(s.split(","))
                .map(String::trim)
                .collect(Collectors.toList()))
            .orElse(Collections.emptyList());

        // schemas初始化逻辑修改，支持Map结构
        Map<String, String> schemasMap = dbconfig.getInclude().getSchemas();
        String dbKey = "default"; // 可根据实际dbName动态赋值
        String schemasStr = null;
        if (schemasMap != null) {
            schemasStr = schemasMap.getOrDefault(dbKey, schemasMap.get("default"));
        }
        schemas = Optional.ofNullable(schemasStr)
            .filter(s -> !s.trim().isEmpty())
            .map(s -> Arrays.stream(s.split(","))
                .map(String::trim)
                .collect(Collectors.toList()))
            .orElse(Collections.emptyList());

        // 初始化断点续跑状态管理器
        resumeStateManager.init();

        // 初始化从节点查询表列表
        initSlaveQueryTbs();
        initTableServices();
        initTb2dbs();
        initTb2Where();
        initTb2Hint();
        initTb2Formula();
        initTb2SumCols();
        initTableInfoMap();
    }


    private void initTb2Hint() {
        tb2hint = new ConcurrentHashMap<>();

        Optional.ofNullable(dbconfig.getHints())
            .ifPresent(hints -> {
                log.info("开始初始化SQL提示映射...");

                // 获取类型映射 - t1, t2 -> 数据库列表
                Map<String, List<String>> typeDbMap = Optional.ofNullable(hints.getType())
                    .map(typeMap -> typeMap.entrySet().stream()
                        .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> StrUtil.split(e.getValue(), ",", true, true)
                        )))
                    .orElse(new HashMap<>());

                // 获取表映射 - t1, t2 -> 表列表
                Map<String, List<String>> typeTableMap = Optional.ofNullable(hints.getTable())
                    .map(tableMap -> tableMap.entrySet().stream()
                        .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> StrUtil.split(e.getValue(), ",", true, true)
                                .stream()
                                .map(String::toUpperCase)
                                .collect(Collectors.toList())
                        )))
                    .orElse(new HashMap<>());

                // 获取SQL提示映射 - t1, t2 -> SQL提示
                Map<String, String> typeSqlMap = Optional.ofNullable(hints.getSql())
                    .orElse(new HashMap<>());

                // 统计要处理的表数量
                long tableCount = typeTableMap.values().stream()
                    .mapToLong(List::size)
                    .sum();

                log.info("找到 {} 个类型提示, {} 个表映射, {} 个SQL提示",
                    typeDbMap.size(), tableCount, typeSqlMap.size());

                // 处理每种类型
                typeTableMap.forEach((type, tables) -> {
                    // 获取该类型的SQL提示
                    String sqlHint = typeSqlMap.getOrDefault(type, "");

                    if (StrUtil.isNotEmpty(sqlHint)) {
                        // 为该类型的所有表设置SQL提示
                        tables.forEach(tableName -> {
                            tb2hint.put(tableName, sqlHint);
                            log.debug("表 [{}] 设置SQL提示: {}", tableName, sqlHint);
                        });

                        log.debug("类型 [{}] 的 {} 个表设置SQL提示: {}",
                            type, tables.size(), sqlHint);
                    } else {
                        log.warn("类型 [{}] 没有对应的SQL提示配置", type);
                    }
                });

                // 检查是否有配置了SQL提示但没有配置表的类型
                typeSqlMap.keySet().stream()
                    .filter(type -> !typeTableMap.containsKey(type))
                    .forEach(type -> log.warn("类型 [{}] 有SQL提示配置但没有对应的表配置", type));

                log.info("SQL提示映射初始化完成，共设置 {} 个表的提示", tb2hint.size());
            });
    }

    private void initSlaveQueryTbs() {
        // 从配置中获取需要从从节点查询的表列表
        slaveQueryTbs = Optional.ofNullable(dbconfig.getSlaveQuery().getTables())
            .map(e -> StrUtil.split(e, ",", true, true))
            .orElse(new CopyOnWriteArrayList<>());
        log.info("初始化从节点查询表列表完成，共 {} 个表: {}", slaveQueryTbs.size(), slaveQueryTbs);
    }

    private void initTb2Where() {
        tb2where = new ConcurrentHashMap<>();

        Map<String, Map<String, String>> whereConfig = dbconfig.getWhere();
        if (whereConfig == null || whereConfig.isEmpty()) {
            log.info("未配置 WHERE 条件");
            return;
        }

        log.info("开始初始化 WHERE 条件映射...");

        // 转换结构：从 db->(tableName->whereStr) 到 tableName->(db->whereStr)
        whereConfig.forEach((db, tableWhereMap) -> tableWhereMap.forEach((tableName, whereStr) -> {
            // 表名转换为大写，保持一致性
            String tableNameUpper = tableName.toUpperCase();

            // 获取或创建该表的 db->whereStr 映射
            Map<String, String> dbToWhereMap = tb2where.computeIfAbsent(
                tableNameUpper, k -> new ConcurrentHashMap<>());

            // 添加该数据库的 WHERE 条件
            dbToWhereMap.put(db, whereStr);

            log.debug("设置表 [{}] 在数据库 [{}] 的 WHERE 条件: {}", tableNameUpper, db, whereStr);
        }));

        log.info("WHERE 条件映射初始化完成，共设置 {} 个表的条件", tb2where.size());
    }

    private void initTb2SumCols() {
        tb2sumCols = new ConcurrentHashMap<>();

        // 所有表都要查询 COUNT（*）作为特殊的SUM列
        tables.forEach((tableName) -> {
            List<String> cols = tb2sumCols.computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>());
            cols.add("_COUNT");
            cols.add("_COUNT_NO_WHERE");  // 添加不带WHERE条件的记录数统计列
        });

        // 如果金额列功能未启用，直接返回
        if (!dbconfig.getSum().isEnable()) {
            log.info("金额列统计功能未启用");
            return;
        }

        int minDecimalDigits = dbconfig.getSum().getMinDecimalDigits();
        log.info("开始初始化金额列映射，最小小数位数: {}", minDecimalDigits);

        // 直接从ora库中批量查询所有表的金额字段
        try {
            // 获取ora库的JdbcTemplate
            String oraDb = "ora";
            JdbcTemplate jdbcTemplate = dynamicJdbcTemplateManager.getJdbcTemplate(oraDb);
            TableService tableService = tableServices.get(oraDb);

            // 获取需要查询的表名列表
            List<String> tableList = new ArrayList<>(tb2dbs.keySet());

            // 从tb2Schema中获取schema
            String schema = schemas.isEmpty() ? null : schemas.get(0);
            if (schema == null) {
                log.warn("未配置schema信息，无法查询金额字段");
                return;
            }

            log.info("开始从ora库批量查询 {} 个表的金额字段信息", tableList.size());

            // 批量查询金额字段
            Map<String, List<String>> decimalColumnsMap = tableService.getDecimalColumnsForTables(
                jdbcTemplate, schema, tableList, minDecimalDigits);

            // 将查询结果保存到tb2sumCols
            decimalColumnsMap.forEach((tableName, columns) -> {
                if (!columns.isEmpty()) {
                    // 不要直接替换现有的列表，而是将金额列添加到已有列表中
                    List<String> existingCols = tb2sumCols.computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>());
                    existingCols.addAll(columns);
                    log.debug("表 [{}] 的金额列: {}", tableName, String.join(",", columns));
                }
            });


            log.info("金额列映射初始化完成，共获取到 {} 个表的金额列信息", tb2sumCols.size());
        } catch (Exception e) {
            log.error("批量查询金额字段失败，将回退到单表查询: {}", e.getMessage(), e);
        }
    }

    private List<TableCsvResult> convertTableInfoToTableCsvResult(TableInfo tableInfo) {
        if (tableInfo == null || tableInfo.getSumResult() == null) {
            log.warn("表 [{}] 的计算结果为空，跳过转换", tableInfo != null ? tableInfo.getTableName() : "未知");
            return Collections.emptyList();
        }

        List<TableCsvResult> results = new ArrayList<>();
        String tableName = tableInfo.getTableName();
        List<String> cols = tableInfo.getSumCols();

        if (cols == null || cols.isEmpty()) {
            log.warn("表 [{}] 没有需要导出的列，跳过转换", tableName);
            return Collections.emptyList();
        }

        // 处理_COUNT_NO_WHERE和_COUNT
        BigDecimal countNoWhere = null;
        Map<String, BigDecimal> countNoWhereResult;

        // 查找_COUNT_NO_WHERE的值
        if (cols.contains("_COUNT_NO_WHERE")) {
            countNoWhereResult = tableInfo.getSumResult().get("_COUNT_NO_WHERE");
            if (countNoWhereResult != null) {
                countNoWhere = countNoWhereResult.getOrDefault("ora", BigDecimal.ZERO);
            }
        }

        // 遍历每一列，但跳过_COUNT_NO_WHERE
        for (String col : cols) {
            // 跳过_COUNT_NO_WHERE，它会在_COUNT行中处理
            if ("_COUNT_NO_WHERE".equals(col)) {
                continue;
            }

            TableCsvResult result = new TableCsvResult();
            result.setTableName(tableName);
            result.setDbs(String.join(",", tableInfo.getDbs()));
            // 过滤掉_COUNT和_COUNT_NO_WHERE这些特殊标记
            List<String> filteredCols = cols.stream()
                .filter(c -> !c.equals("_COUNT") && !c.equals("_COUNT_NO_WHERE"))
                .collect(Collectors.toList());
            result.setSumCols(String.join(",", filteredCols));
            result.setCol(col);

            // 获取当前列在各数据库中的求和结果
            Map<String, BigDecimal> colResult = tableInfo.getSumResult().get(col);
            if (colResult == null) {
                log.warn("表 [{}] 列 [{}] 的求和结果为空，跳过", tableName, col);
                continue;
            }

            // 如果当前列是_COUNT，则设置sumOraAll为_COUNT_NO_WHERE的值
            if ("_COUNT".equals(col)) {
                result.setSumOraAll(countNoWhere);
            }

            // 设置各数据库的求和结果
            result.setSumOra(colResult.get("ora"));
            result.setSumRlcmsBase(colResult.get("rlcms-base"));
            result.setSumRlcmsPv1(colResult.get("rlcms-pv1"));
            result.setSumRlcmsPv2(colResult.get("rlcms-pv2"));
            result.setSumRlcmsPv3(colResult.get("rlcms-pv3"));
            result.setSumBscopyPv1(colResult.get("bscopy-pv1"));
            result.setSumBscopyPv2(colResult.get("bscopy-pv2"));
            result.setSumBscopyPv3(colResult.get("bscopy-pv3"));

            // 如果存在公式，设置公式相关字段
            Formula formula = tableInfo.getFormula();
            if (formula != null) {
                result.setFormula(formula.getDesc());
                result.setFormulaResult(formula.result(tableInfo, col) ? "通过" : "不通过");
                result.setDiff(formula.diff(tableInfo, col));
                result.setDiffDesc(formula.diffDesc(tableInfo, col));
            }

            results.add(result);
        }

        return results;
    }

    private void initTableInfoMap() {
        tableInfoMap = new ConcurrentHashMap<>();
        tb2dbs.forEach((tableName, dbs) -> tableInfoMap.put(tableName, new TableInfo(tableName, dbs)));

        // 设置表的公式信息
        log.info("开始设置表的公式信息...");
        final int[] formulaCount = {0};
        tb2formula.forEach((tableName, formula) -> {
            TableInfo tableInfo = tableInfoMap.get(tableName);
            if (tableInfo != null) {
                tableInfo.setFormula(formula);
                formulaCount[0]++;
                log.debug("表[{}]设置公式: {}", tableName, formula.getDesc());
            } else {
                log.warn("表[{}]在tableInfoMap中不存在，但在tb2formula中存在", tableName);
            }
        });
        log.info("公式信息设置完成，共设置 {} 个表的公式", formulaCount[0]);

        // 修复NullPointerException：只设置tableInfoMap中存在的表的sumCols
        tb2sumCols.forEach((tableName, sumCols) -> {
            TableInfo tableInfo = tableInfoMap.get(tableName);
            if (tableInfo != null) {
                tableInfo.setSumCols(sumCols);
            } else {
                log.warn("表[{}]在tableInfoMap中不存在，但在tb2sumCols中存在", tableName);
            }
        });

        log.info("开始计算表列求和结果...");

        // 获取总表数量
        int totalTables = tb2dbs.size();
        log.info("共需处理 {} 张表的数据检查", totalTables);

        // 初始化断点续跑状态管理器的表列表
        resumeStateManager.initTableLists(tb2dbs.keySet(), totalTables);

        // 创建全局的StopWatch来记录所有表处理的总时间
        globalTableWatch = new StopWatch("全部表处理");
        globalTableWatch.start("所有表处理任务开始");

        // 初始化CSV导出
        csvExportManager.initCsvExport(totalTables);

        // 并行处理每个表
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        tb2dbs.forEach((tableName, dbList) -> {
            // 检查表是否已经在断点续跑中完成处理
            if (resumeStateManager.isTableCompleted(tableName)) {
                log.info("表[{}]已在之前的运行中完成处理，跳过", tableName);
                return;
            }

            List<String> sumCols = tb2sumCols.get(tableName);
            if (sumCols == null || sumCols.isEmpty()) {
                log.info("表[{}]没有需要求和的列，标记为已完成并跳过", tableName);
                resumeStateManager.markTableCompleted(tableName, totalTables);
                return;
            }

            // 标记表为进行中状态
            resumeStateManager.markTableProcessing(tableName);

            // 异步处理每个表 - 使用表处理专用线程池
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                // 为每个表创建一个StopWatch并保存到Map中
                StopWatch tableWatch = new StopWatch(StrUtil.format("表[{}]处理", tableName));
                tableStopWatches.put(tableName, tableWatch);
                tableWatch.start(StrUtil.format("表[{}]处理开始", tableName));
                
                try {
                    TableInfo tableInfo = tableInfoMap.get(tableName);
                    Map<String, Map<String, BigDecimal>> sumResult = new ConcurrentHashMap<>();

                    // 为每列初始化结果Map
                    for (String sumCol : sumCols) {
                        sumResult.put(sumCol, new ConcurrentHashMap<>());
                    }

                    // 创建数据库查询的CompletableFuture列表
                    List<CompletableFuture<Void>> dbFutures = new ArrayList<>();

                    // 为每个数据库创建异步查询任务
                    for (String db : dbList) {
                        // 检查是否需要从从节点查询
                        String actualDb = db;
                        if ("ora".equals(db) && slaveQueryTbs.contains(tableName)) {
                            actualDb = "ora-slave";
                            log.debug("表[{}]将从从节点[{}]查询", tableName, actualDb);
                        }

                        final String finalDb = db; // 原始数据库名，用于结果存储
                        final String finalActualDb = actualDb; // 实际查询的数据库名

                        // 使用数据库查询专用线程池处理查询任务
                        CompletableFuture<Void> dbFuture = CompletableFuture.runAsync(() -> {
                            // 为每个数据库查询创建一个StopWatch并保存到Map中
                            Map<String, StopWatch> dbWatches = dbQueryStopWatches.computeIfAbsent(
                                tableName, k -> new ConcurrentHashMap<>());
                            StopWatch sqlWatch = new StopWatch(StrUtil.format("表[{}]-数据库[{}]查询", tableName, finalActualDb));
                            dbWatches.put(finalActualDb, sqlWatch);
                            
                            sqlWatch.start(StrUtil.format("表[{}]-数据库[{}]构建SQL", tableName, finalActualDb));
                            
                            try {
                                JdbcTemplate jdbcTemplate = dynamicJdbcTemplateManager.getJdbcTemplate(finalActualDb);

                                // 构建合并的查询语句
                                StringBuilder sqlBuilder = new StringBuilder("SELECT ");

                                // 检查该表是否有SQL提示，如果有则添加到查询开头
                                String sqlHint = tb2hint.get(tableName);
                                if (StrUtil.isNotEmpty(sqlHint)) {
                                    sqlBuilder.append(sqlHint).append(" ");
                                    log.debug("为表[{}]添加SQL提示: {}", tableName, sqlHint);
                                }

                                boolean hasCountCol = false;
                                boolean hasCountNoWhereCol = false;

                                // 检查是否存在WHERE条件
                                Map<String, String> dbWhereMap = tb2where.get(tableName);
                                String whereCondition = dbWhereMap != null ? dbWhereMap.get(finalDb) : null;
                                boolean hasWhereCondition = whereCondition != null && !whereCondition.trim().isEmpty();

                                // 收集所有需要SUM的列和COUNT
                                for (String sumCol : sumCols) {
                                    if ("_COUNT_NO_WHERE".equals(sumCol)) {
                                        // 总是计算不带WHERE的COUNT
                                        sqlBuilder.append("COUNT(*) AS \"_COUNT_NO_WHERE\", ");
                                        hasCountNoWhereCol = true;
                                    } else if ("_COUNT".equals(sumCol)) {
                                        // 根据是否有WHERE条件决定如何计算COUNT
                                        if (hasWhereCondition) {
                                            sqlBuilder.append("SUM(CASE WHEN ").append(whereCondition)
                                                .append(" THEN 1 ELSE 0 END) AS \"_COUNT\", ");
                                        } else {
                                            sqlBuilder.append("COUNT(*) AS \"_COUNT\", ");
                                        }
                                        hasCountCol = true;
                                    } else {
                                        // 根据是否有WHERE条件决定如何计算SUM
                                        if (hasWhereCondition) {
                                            sqlBuilder.append("SUM(CASE WHEN ").append(whereCondition)
                                                .append(" THEN ").append(sumCol).append(" ELSE 0 END) AS ")
                                                .append(sumCol).append(", ");
                                        } else {
                                            sqlBuilder.append("SUM(").append(sumCol).append(") AS ")
                                                .append(sumCol).append(", ");
                                        }
                                    }
                                }

                                // 移除最后一个逗号和空格
                                if (!sumCols.isEmpty()) {
                                    sqlBuilder.setLength(sqlBuilder.length() - 2);
                                }

                                // 直接使用表名，不加schema前缀
                                sqlBuilder.append(" FROM ").append(tableName);

                                String sql = sqlBuilder.toString();
                                sqlWatch.stop();
                                log.debug("表[{}]-数据库[{}]的SQL构建完成，耗时: {}ms", tableName, finalActualDb, sqlWatch.getLastTaskTimeMillis());
                                
                                sqlWatch.start(StrUtil.format("表[{}]-数据库[{}]执行SQL", tableName, finalActualDb));
                                log.debug("执行合并统计SQL: {}, 数据库: {} (实际查询: {})", sql, finalDb, finalActualDb);

                                // 将外部变量复制为final变量，以便lambda表达式中使用
                                final boolean finalHasCountCol = hasCountCol;
                                final boolean finalHasCountNoWhereCol = hasCountNoWhereCol;

                                // 执行查询并处理结果
                                jdbcTemplate.query(sql, rs -> {
                                    // 处理所有SUM列的结果
                                    for (String sumCol : sumCols) {
                                        BigDecimal value;
                                        if ("_COUNT".equals(sumCol) && finalHasCountCol) {
                                            value = new BigDecimal(rs.getLong("_COUNT"));
                                        } else if ("_COUNT_NO_WHERE".equals(sumCol) && finalHasCountNoWhereCol) {
                                            value = new BigDecimal(rs.getLong("_COUNT_NO_WHERE"));
                                        } else {
                                            value = rs.getBigDecimal(sumCol);
                                        }

                                        // 处理NULL值
                                        if (value == null) {
                                            value = BigDecimal.ZERO;
                                        }

                                        // 保存结果 - 注意：结果存储到原始数据库名下，而不是实际查询的数据库
                                        sumResult.get(sumCol).put(finalDb, value);
                                        log.debug("表[{}]列[{}]在数据库[{}]的求和结果: {} (实际查询: {})",
                                            tableName, sumCol, finalDb, value, finalActualDb);
                                    }
                                });
                                sqlWatch.stop();
                                
                                // 从Map中获取StopWatch对象
                                StopWatch currentWatch = dbQueryStopWatches
                                    .getOrDefault(tableName, new ConcurrentHashMap<>())
                                    .getOrDefault(finalActualDb, sqlWatch);
                                    
                                // 记录表在当前数据库的处理时间
                                long dbProcessTime = currentWatch.getLastTaskTimeMillis();
                                resumeStateManager.recordTableDbTime(tableName, finalActualDb, dbProcessTime);
                                
                                log.debug("表[{}]在数据库[{}]的SQL执行完成，SQL耗时: {}ms",
                                    tableName, finalActualDb, dbProcessTime);
                            } catch (Exception e) {
                                // 从Map中获取StopWatch对象
                                StopWatch currentWatch = dbQueryStopWatches
                                    .getOrDefault(tableName, new ConcurrentHashMap<>())
                                    .getOrDefault(finalActualDb, sqlWatch);
                                
                                if (currentWatch.isRunning()) {
                                    currentWatch.stop();
                                }
                                
                                log.error("计算表[{}]在数据库[{}]的列求和时发生错误 (实际查询: {}): {}, SQL耗时: {}ms",
                                    tableName, finalDb, finalActualDb, e.getMessage(), currentWatch.getLastTaskTimeMillis(), e);

                                // 出错时为所有列设为0
                                for (String sumCol : sumCols) {
                                    sumResult.get(sumCol).put(finalDb, BigDecimal.ZERO);
                                }
                            }
                        }, dbQueryExecutor); // 使用数据库查询专用线程池

                        dbFutures.add(dbFuture);
                    }

                    // 从Map中获取该表的StopWatch
                    StopWatch currentTableWatch = tableStopWatches.getOrDefault(tableName, tableWatch);
                    
                    // 在停止前确保任务已经启动，并在后续再次启动前停止之前的任务
                    if (currentTableWatch.isRunning()) {
                        currentTableWatch.stop();
                    }
                    currentTableWatch.start(StrUtil.format("表[{}]等待所有数据库查询完成", tableName));
                    
                    // 等待所有数据库查询完成
                    CompletableFuture.allOf(dbFutures.toArray(new CompletableFuture[0]))
                        .exceptionally(e -> {
                            log.error("表[{}]的数据库查询任务中有错误发生: {}", tableName, e.getMessage(), e);
                            return null;
                        })
                        .join();
                    
                    if (currentTableWatch.isRunning()) {
                        currentTableWatch.stop();
                    }

                    // 设置结果
                    currentTableWatch.start(StrUtil.format("表[{}]设置统计结果", tableName));
                    tableInfo.setSumResult(sumResult);
                    
                    if (currentTableWatch.isRunning()) {
                        currentTableWatch.stop();
                    }
                    
                    log.debug("表[{}]的求和计算完成, 共计算 {} 列", tableName, sumCols.size());
                } catch (Exception e) {
                    log.error("表[{}]的求和处理过程中发生错误: {}", tableName, e.getMessage(), e);
                } finally {
                    // 获取StopWatch并确保停止
                    StopWatch currentTableWatch = tableStopWatches.getOrDefault(tableName, tableWatch);
                    // 确保停止当前任务
                    if (currentTableWatch.isRunning()) {
                        currentTableWatch.stop();
                    }
                    log.info("表[{}]处理完成，总耗时统计：{}ms", tableName, currentTableWatch.getTotalTimeMillis());
                    
                    // 处理完成后从Map中移除，避免内存泄漏
                    tableStopWatches.remove(tableName);
                    dbQueryStopWatches.remove(tableName);
                }
            }, tableExecutor) // 使用表处理专用线程池
            .thenAcceptAsync(unused -> { // 使用CSV导出专用线程池
                // 为每个CSV导出创建一个StopWatch并保存到Map中
                StopWatch csvWatch = new StopWatch(StrUtil.format("表[{}]CSV导出", tableName));
                csvExportStopWatches.put(tableName, csvWatch);
                csvWatch.start(StrUtil.format("表[{}]生成CSV数据", tableName));
                
                // 将TableInfo转换为TableCsvResult并导出到CSV
                try {
                    TableInfo tableInfo = tableInfoMap.get(tableName);
                    if (tableInfo == null || tableInfo.getSumResult() == null) {
                        log.warn("表[{}]的计算结果为空，跳过CSV导出", tableName);
                        return;
                    }

                    List<TableCsvResult> results = convertTableInfoToTableCsvResult(tableInfo);

                    // 如果没有结果，跳过导出
                    if (results.isEmpty()) {
                        log.warn("表[{}]没有有效的求和结果，跳过CSV导出", tableName);
                        return;
                    }

                    // 使用CSV导出管理器导出数据
                    csvExportManager.exportTableToCsv(tableName, results, totalTables);

                    // 保存结果到内存映射
                    tableCsvResultMap.put(tableName, results);
                    
                    // 标记该表已处理完成，并保存状态
                    resumeStateManager.markTableCompleted(tableName, totalTables);
                    
                    // 获取当前CSV的StopWatch
                    StopWatch currentCsvWatch = csvExportStopWatches.getOrDefault(tableName, csvWatch);
                    if (currentCsvWatch.isRunning()) {
                        currentCsvWatch.stop();
                    }
                    
                    // 保存表信息数据
                    resumeStateManager.saveState(tableInfoMap);
                    
                    log.debug("表[{}]的CSV导出已完成并保存状态，CSV导出耗时: {}ms",
                        tableName, currentCsvWatch.getLastTaskTimeMillis());
                    
                    // 处理完成后从Map中移除，避免内存泄漏
                    csvExportStopWatches.remove(tableName);
                } catch (Exception e) {
                    // 获取当前CSV的StopWatch
                    StopWatch currentCsvWatch = csvExportStopWatches.getOrDefault(tableName, csvWatch);
                    // 确保停止计时
                    if (currentCsvWatch.isRunning()) {
                        currentCsvWatch.stop();
                    }
                    log.error("处理表[{}]的数据时发生错误，CSV处理耗时: {}ms: {}", 
                        tableName, currentCsvWatch.getLastTaskTimeMillis(), e.getMessage(), e);
                    
                    // 处理完成后从Map中移除，避免内存泄漏
                    csvExportStopWatches.remove(tableName);
                }
            }, csvExportExecutor); // 使用CSV导出专用线程池

            futures.add(future);
        });

        // 等待所有任务完成
        futures.stream()
            .collect(Collectors.collectingAndThen(
                Collectors.toList(),
                fs -> CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
            )).join();

        // 关闭CSV写入器
        csvExportManager.closeWriter();
        
        // 停止全局表处理计时
        if (globalTableWatch != null && globalTableWatch.isRunning()) {
            globalTableWatch.stop();
        }
        log.info("所有表处理完成，总耗时：{}s", globalTableWatch != null ? globalTableWatch.getTotalTimeSeconds() : "无法获取");
        
        // 记录总结信息
        ResumeState state = resumeStateManager.getCurrentState();
        log.info("所有任务处理完成。共处理 {} 张表，完成 {} 张表",
                state.getTotalTables(), 
                state.getCompletedCount());
                
        // 任务全部完成后，关闭所有线程池，避免程序不退出
        shutdownExecutors();
    }

    /**
     * 关闭所有线程池，允许程序正常退出
     */
    private void shutdownExecutors() {
        log.info("所有数据处理任务已完成，开始关闭线程池...");
        
        try {
            // 保存最终状态并关闭状态管理器
            try {
                log.debug("保存最终状态并关闭状态管理器...");
                // 确保最终状态已保存，传入tableInfoMap
                resumeStateManager.saveState(tableInfoMap);
                // 等待一段时间确保状态保存完成
                Thread.sleep(1000);
                // 关闭状态管理器
                resumeStateManager.shutdown();
            } catch (Exception e) {
                log.error("关闭状态管理器时发生错误: {}", e.getMessage(), e);
            }

            // 直接关闭线程池
            if (tableExecutor != null) {
                log.debug("关闭表处理线程池...");
                tableExecutor.shutdown();
            }

            if (dbQueryExecutor != null) {
                log.debug("关闭数据库查询线程池...");
                dbQueryExecutor.shutdown();
            }

            if (csvExportExecutor != null) {
                log.debug("关闭CSV导出线程池...");
                csvExportExecutor.shutdown();
            }

            log.info("线程池已关闭");
        } catch (Exception e) {
            log.error("关闭线程池时发生错误: {}", e.getMessage(), e);
        }
    }

    private void initTb2Formula() {
        tb2formula = new ConcurrentHashMap<>();

        log.info("开始初始化公式映射...");

        // 获取所有公式配置
        addFormulaTables(dbconfig.getFormula().getFormula1(), new Formula1());
        addFormulaTables(dbconfig.getFormula().getFormula2(), new Formula2());
        addFormulaTables(dbconfig.getFormula().getFormula3(), new Formula3());
        addFormulaTables(dbconfig.getFormula().getFormula4(), new Formula4());
        addFormulaTables(dbconfig.getFormula().getFormula5(), new Formula5());
        addFormulaTables(dbconfig.getFormula().getFormula6(), new Formula6());

        log.info("公式映射初始化完成，共设置 {} 个表的公式", tb2formula.size());
    }

    private void addFormulaTables(String tablesStr, Formula formula) {
        if (tablesStr == null || tablesStr.trim().isEmpty()) {
            return;
        }

        String[] tables = tablesStr.split(",");
        for (String table : tables) {
            String tableName = table.trim().toUpperCase();
            tb2formula.put(tableName, formula);
            log.debug("表 [{}] 设置公式: {}", tableName, formula.getDesc());
        }
    }

    private void initTableServices() {
        tableServices = new ConcurrentHashMap<>();
        for (String db : dbs) {
            try {
                DynamicDataSourceManager dataSourceManager = SpringUtil.getBean(DynamicDataSourceManager.class);
                // 首先检查数据源是否存在(已启用)
                if (!dataSourceManager.hasDataSource(db)) {
                    log.warn("数据源 [{}] 不存在或已禁用，将使用禁用的TableService", db);
                    tableServices.put(db, new TableService.DisabledTableService(db));
                } else {
                    tableServices.put(db, TableService.getTableService(db));
                    log.debug("为数据源 [{}] 创建TableService成功", db);
                }
            } catch (Exception e) {
                log.error("为数据源 [{}] 创建TableService失败: {}", db, e.getMessage());
                // 创建失败时使用禁用的TableService
                tableServices.put(db, new TableService.DisabledTableService(db));
            }
        }
    }

    private void initTb2dbs() {
        tb2dbs = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String db : dbs) {
            // 跳过ora-slave，不参与tb2dbs的初始化
            if ("ora-slave".equals(db)) {
                log.info("跳过从节点 [ora-slave] 的表信息初始化");
                continue;
            }

            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                log.info("开始查询数据库 [{}] 中的表信息", db);
                try {
                    JdbcTemplate jdbcTemplate = dynamicJdbcTemplateManager.getJdbcTemplate(db);
                    TableService tableService = tableServices.get(db);
                    // 根据dbName获取schemas字符串并转为List
                    Map<String, String> schemasMap = dbconfig.getInclude().getSchemas();
                    String schemasStr = schemasMap != null ? schemasMap.getOrDefault(db, schemasMap.get("default")) : null;
                    List<String> schemas = schemasStr == null ? Collections.emptyList() : Arrays.stream(schemasStr.split(",")).map(String::trim).collect(Collectors.toList());
                    List<TableEnt> tables = tableService.getTables(jdbcTemplate, schemas, this.tables);
                    log.info("数据库 [{}] 中查询到 {} 个表", db, tables.size());
                    return tables;
                } catch (Exception e) {
                    log.error("查询数据库 [{}] 中的表信息失败: {}", db, e.getMessage(), e);
                    return ListUtil.<TableEnt>empty();
                }
            }, dbQueryExecutor) // 使用数据库查询专用线程池
            .thenAcceptAsync(tableQueryed ->
                tableQueryed.forEach(tableEnt -> {
                        tb2dbs.computeIfAbsent(tableEnt.getTableName(), k -> new CopyOnWriteArrayList<>()).add(db);
                        // 将表的schema信息保存到tb2Schema映射中
                        tb2Schema.putIfAbsent(tableEnt.getTableName(), tableEnt.getSchemaName());
                    }
                ), tableExecutor // 使用表处理专用线程池处理数据收集
            );

            futures.add(future);
        }

        // 等待所有任务完成
        futures.stream()
            .collect(Collectors.collectingAndThen(
                Collectors.toList(),
                fs -> CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
            ))
            .thenRun(() -> log.info("所有数据库表信息查询完成，共获取到{}个表信息", tb2dbs.size()))
            .exceptionally(e -> {
                log.error("等待数据库表信息查询时发生错误: {}", e.getMessage(), e);
                return null;
            })
            .join();
    }
}

