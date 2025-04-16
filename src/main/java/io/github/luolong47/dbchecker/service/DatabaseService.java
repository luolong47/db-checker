package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import io.github.luolong47.dbchecker.manager.TableInfoManager;
import io.github.luolong47.dbchecker.manager.ThreadPoolManager;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据库服务类，用于测试多数据源
 * @author LuoLong
 */
@Slf4j
@Service
public class DatabaseService {

    public static final BigDecimal THRESHOLD = new BigDecimal("100000000000000");
    private final JdbcTemplate oraJdbcTemplate;
    private final JdbcTemplate rlcmsBaseJdbcTemplate;
    private final JdbcTemplate rlcmsPv1JdbcTemplate;
    private final JdbcTemplate rlcmsPv2JdbcTemplate;
    private final JdbcTemplate rlcmsPv3JdbcTemplate;
    private final JdbcTemplate bscopyPv1JdbcTemplate;
    private final JdbcTemplate bscopyPv2JdbcTemplate;
    private final JdbcTemplate bscopyPv3JdbcTemplate;
    private final DbConfig dbConfig;
    private final TableInfoManager tableInfoManager;
    private final ResumeStateManager resumeStateManager;
    private final FormulaCalculationService formulaCalculationService;
    private final TableMetadataService tableMetadataService;
    private final ThreadPoolManager threadPoolManager;

    // 任务队列和计数器
    private ConcurrentLinkedQueue<CompletableFuture<Void>> csvExportTasks;
    private AtomicInteger pendingExportTasks;
    // 已处理的表集合
    private final Set<String> processedTables = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public DatabaseService(
        @Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate,
        @Qualifier("oraSlaveJdbcTemplate") JdbcTemplate oraSlaveJdbcTemplate,
        @Qualifier("rlcmsBaseJdbcTemplate") JdbcTemplate rlcmsBaseJdbcTemplate,
        @Qualifier("rlcmsPv1JdbcTemplate") JdbcTemplate rlcmsPv1JdbcTemplate,
        @Qualifier("rlcmsPv2JdbcTemplate") JdbcTemplate rlcmsPv2JdbcTemplate,
        @Qualifier("rlcmsPv3JdbcTemplate") JdbcTemplate rlcmsPv3JdbcTemplate,
        @Qualifier("bscopyPv1JdbcTemplate") JdbcTemplate bscopyPv1JdbcTemplate,
        @Qualifier("bscopyPv2JdbcTemplate") JdbcTemplate bscopyPv2JdbcTemplate,
        @Qualifier("bscopyPv3JdbcTemplate") JdbcTemplate bscopyPv3JdbcTemplate,
        DbConfig dbConfig,
        TableInfoManager tableInfoManager,
        ResumeStateManager resumeStateManager,
        FormulaCalculationService formulaCalculationService,
        TableMetadataService tableMetadataService,
        ThreadPoolManager threadPoolManager) {
        this.oraJdbcTemplate = oraJdbcTemplate;
        this.rlcmsBaseJdbcTemplate = rlcmsBaseJdbcTemplate;
        this.rlcmsPv1JdbcTemplate = rlcmsPv1JdbcTemplate;
        this.rlcmsPv2JdbcTemplate = rlcmsPv2JdbcTemplate;
        this.rlcmsPv3JdbcTemplate = rlcmsPv3JdbcTemplate;
        this.bscopyPv1JdbcTemplate = bscopyPv1JdbcTemplate;
        this.bscopyPv2JdbcTemplate = bscopyPv2JdbcTemplate;
        this.bscopyPv3JdbcTemplate = bscopyPv3JdbcTemplate;
        this.dbConfig = dbConfig;
        this.tableInfoManager = tableInfoManager;
        this.resumeStateManager = resumeStateManager;
        this.formulaCalculationService = formulaCalculationService;
        this.tableMetadataService = tableMetadataService;
        this.threadPoolManager = threadPoolManager;
    }

    /**
     * 初始化服务
     */
    @PostConstruct
    public void init() {
        log.info("初始化数据库服务...");
        log.debug("配置信息 - 导出目录: {}", dbConfig.getExportDirectory());
        log.debug("配置信息 - 运行模式: {}", dbConfig.getRunMode());
        log.debug("配置信息 - 包含表: {}", dbConfig.getIncludeTables());
        log.debug("配置信息 - 包含Schema: {}", dbConfig.getIncludeSchemas());

        // 根据运行模式初始化状态
        resumeStateManager.initResumeState(dbConfig.getRunMode());

        // 初始化任务队列和计数器
        csvExportTasks = new ConcurrentLinkedQueue<>();
        pendingExportTasks = new AtomicInteger(0);
        // 清空已处理表集合
        processedTables.clear();

        log.info("数据库服务初始化完成");
    }

    /**
     * 获取需要处理的数据库列表
     */
    private Map<String, JdbcTemplate> getDatabasesToProcess() {
        // 全部可用的数据库列表
        Map<String, JdbcTemplate> allDatabases = new LinkedHashMap<>();
        allDatabases.put("ora", oraJdbcTemplate);
        // ora-slave只作为查询替代，不直接处理，所以不添加到处理列表中
        // allDatabases.put("ora-slave", oraSlaveJdbcTemplate);
        allDatabases.put("rlcms_base", rlcmsBaseJdbcTemplate);
        allDatabases.put("rlcms_pv1", rlcmsPv1JdbcTemplate);
        allDatabases.put("rlcms_pv2", rlcmsPv2JdbcTemplate);
        allDatabases.put("rlcms_pv3", rlcmsPv3JdbcTemplate);
        allDatabases.put("bscopy_pv1", bscopyPv1JdbcTemplate);
        allDatabases.put("bscopy_pv2", bscopyPv2JdbcTemplate);
        allDatabases.put("bscopy_pv3", bscopyPv3JdbcTemplate);

        return resumeStateManager.filterDatabases(allDatabases, dbConfig.getRunMode(), dbConfig.getRerunDatabases());
    }

    /**
     * 导出金额字段的SUM值到CSV文件
     */
    public void exportMoneyFieldSumToCsv() throws IOException {
        log.info("当前运行模式: {}", dbConfig.getRunMode());
        if (!cn.hutool.core.util.StrUtil.isEmpty(dbConfig.getRerunDatabases())) {
            log.info("指定重跑数据库: {}", dbConfig.getRerunDatabases());
        }

        // 重置CSV导出任务状态
        csvExportTasks.clear();
        pendingExportTasks.set(0);
        processedTables.clear();

        // 检查现有状态
        boolean hasRestoredTableInfo = tableInfoManager.hasTableInfo();

        if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(dbConfig.getRunMode())) {
            log.info("已从断点续跑文件恢复{}个表的信息，将继续处理未完成的数据库和表", tableInfoManager.getTableCount());
        } else {
            log.info("开始收集表信息...");
            // 如果没有恢复数据或不是断点续跑模式，则清空已有的表信息
            if (!hasRestoredTableInfo || !"RESUME".equalsIgnoreCase(dbConfig.getRunMode())) {
                tableInfoManager.clearTableInfo();
            }
        }

        // 获取需要处理的数据库
        Map<String, JdbcTemplate> databasesToProcess = getDatabasesToProcess();
        log.info("本次将处理{}个数据库: {}", databasesToProcess.size(), StrUtil.join(", ", databasesToProcess.keySet()));

        // 创建结果目录
        File directory = new File(dbConfig.getExportDirectory());
        if (!directory.exists()) {
            cn.hutool.core.io.FileUtil.mkdir(directory);
        }

        // 创建输出文件
        File outputFile = cn.hutool.core.io.FileUtil.file(directory,
            cn.hutool.core.util.StrUtil.format("表金额字段SUM比对-{}.csv", DateUtil.format(DateUtil.date(), "yyyyMMdd-HHmmss")));

        // 初始化CSV文件，写入表头
        initCsvFile(outputFile);

        // 使用数据库专用线程池并发获取表信息
        List<CompletableFuture<Void>> dbFutures = new ArrayList<>();

        for (Map.Entry<String, JdbcTemplate> entry : databasesToProcess.entrySet()) {
            String dbName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();

            // 如果数据库已经处理过且从断点续跑恢复了数据，则跳过
            if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(dbConfig.getRunMode())
                && resumeStateManager.isDatabaseProcessed(dbName)) {
                log.info("数据库[{}]已在上次运行中处理，跳过", dbName);
                continue;
            }

            // 获取当前数据库的专用线程池
            ExecutorService dbExecutor = threadPoolManager.getDbThreadPool(dbName);

            // 在数据库专用线程池中执行表信息获取
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                log.info("使用[{}]线程池开始处理数据库[{}]", Thread.currentThread().getName(), dbName);
                List<TableInfo> tables = tableMetadataService.getTablesInfoFromDataSource(
                    jdbcTemplate, dbName, dbConfig.getIncludeSchemas(), dbConfig.getIncludeTables(),
                    dbConfig, resumeStateManager, dbConfig.getRunMode());
                log.info("从{}数据源获取到{}个表", dbName, tables.size());
                return tables;
            }, dbExecutor).thenAccept(tables -> {
                try {
                    if (tables.isEmpty()) {
                        return;
                    }

                    TableInfo firstTable = tables.get(0);
                    String dataSourceName = firstTable.getDataSourceName();

                    // 添加表信息到映射
                    tableInfoManager.addTableInfoList(tables, dataSourceName);

                    // 对于每张表，检查是否已收集完所有数据源的数据，并尝试导出(异步)
                    for (TableInfo table : tables) {
                        checkAndQueueExportTable(table.getTableName(), processedTables, outputFile);
                    }

                    // 及时保存状态
                    resumeStateManager.saveResumeState(tableInfoManager.getTableInfoMap());
                } catch (Exception e) {
                    log.error("处理表信息时出错: {}", e.getMessage(), e);
                }
            });

            dbFutures.add(future);
        }

        // 等待所有表信息处理完成
        CompletableFuture.allOf(dbFutures.toArray(new CompletableFuture[0])).join();

        log.info("表信息收集完成，共发现{}个表", tableInfoManager.getTableCount());

        // 异步处理所有尚未导出的表
        queueRemainingTables(processedTables, outputFile);

        // 等待所有CSV导出任务完成
        log.info("正在等待所有CSV导出任务完成，剩余{}个任务...", pendingExportTasks.get());
        waitForCsvExportTasks();

        // 在最后，确保所有处理都被标记为已完成
        resumeStateManager.saveResumeState(tableInfoManager.getTableInfoMap());
    }

    /**
     * 等待所有CSV导出任务完成
     */
    private void waitForCsvExportTasks() {
        CompletableFuture<?>[] tasks = csvExportTasks.toArray(new CompletableFuture<?>[0]);
        if (tasks.length > 0) {
            CompletableFuture.allOf(tasks).join();
        }
    }

    /**
     * 检查并队列化导出单个表（异步方式）
     */
    private void checkAndQueueExportTable(String tableName, Set<String> processedTables, File outputFile) {
        // 快速检查表是否已处理
        if (processedTables.contains(tableName)) {
            return;
        }

        // 获取表信息 - ConcurrentHashMap读取无需同步
        TableInfo tableInfo = tableInfoManager.getTableInfoMap().get(tableName);
        if (tableInfo == null) {
            return;
        }

        // 获取表所需的所有数据源
        Set<String> requiredDataSources = new HashSet<>();
        for (String dbName : getDatabasesToProcess().keySet()) {
            if (tableMetadataService.shouldProcessTable(dbName, tableName, dbConfig)) {
                requiredDataSources.add(dbName);
            }
        }

        // 检查该表是否已在所有必需的数据源中收集了数据
        Set<String> collectedDataSources = new HashSet<>(tableInfo.getDataSources());

        // 如果所有必需的数据源都已收集，则尝试导出
        if (collectedDataSources.containsAll(requiredDataSources)) {
            // 使用ConcurrentHashMap的原子性操作来检查和添加
            if (processedTables.add(tableName)) {
                log.info("表[{}]已收集完所有必需的数据源数据，准备队列化导出", tableName);

                // 创建只包含此表的Map
                Map<String, TableInfo> singleTableMap = new HashMap<>();
                singleTableMap.put(tableName, tableInfo);

                // 提交异步导出任务而不是直接导出
                queueCsvExportTask(singleTableMap, outputFile, tableName);
            }
        }
    }

    /**
     * 队列化所有剩余尚未导出的表（异步处理）
     */
    private void queueRemainingTables(Set<String> processedTables, File outputFile) {
        Map<String, TableInfo> tableInfoMap = tableInfoManager.getTableInfoMap();

        // 筛选出尚未处理的表
        Map<String, TableInfo> remainingTables = new HashMap<>();

        // 直接遍历并检查，无需同步，因为使用了线程安全的集合
        for (Map.Entry<String, TableInfo> entry : tableInfoMap.entrySet()) {
            if (!processedTables.contains(entry.getKey())) {
                remainingTables.put(entry.getKey(), entry.getValue());
            }
        }

        if (!remainingTables.isEmpty()) {
            log.info("发现{}个表尚未导出，开始分批队列化导出", remainingTables.size());

            // 分批处理剩余表以减少内存压力
            List<String> tableNames = new ArrayList<>(remainingTables.keySet());
            // 每批处理10张表
            int batchSize = 10;
            // 向上取整
            int batches = (tableNames.size() + batchSize - 1) / batchSize;

            log.info("将{}个剩余表分为{}批处理", tableNames.size(), batches);

            // 将所有表添加到已处理集合
            processedTables.addAll(remainingTables.keySet());

            // 按批次提交任务
            for (int i = 0; i < batches; i++) {
                int start = i * batchSize;
                int end = Math.min(start + batchSize, tableNames.size());
                Map<String, TableInfo> batchTables = new HashMap<>();

                for (int j = start; j < end; j++) {
                    String tableName = tableNames.get(j);
                    batchTables.put(tableName, remainingTables.get(tableName));
                }

                // 提交批次任务
                queueCsvExportTask(batchTables, outputFile, "batch-" + (i + 1));
                log.debug("已提交第{}批剩余表导出任务，包含{}个表", i + 1, batchTables.size());
            }
        }
    }

    /**
     * 将CSV导出任务添加到队列
     */
    private void queueCsvExportTask(Map<String, TableInfo> tables, File outputFile, String taskId) {
        pendingExportTasks.incrementAndGet();

        // 获取CSV导出线程池
        ExecutorService csvExportExecutor = threadPoolManager.getCsvExportExecutor();
        
        CompletableFuture<Void> exportTask = CompletableFuture.supplyAsync(() -> {
            try {
                log.debug("任务[{}]开始准备导出数据，包含{}个表", taskId, tables.size());
                // 生成导出数据
                return formulaCalculationService.prepareExportData(tables);
            } catch (Exception e) {
                log.error("准备导出数据时出错[{}]: {}", taskId, e.getMessage(), e);
                return new ArrayList<MoneyFieldSumInfo>();
            }
        }, csvExportExecutor).thenAcceptAsync(dataList -> {
            try {
                if (!dataList.isEmpty()) {
                    log.debug("任务[{}]准备写入{}行数据到CSV文件", taskId, dataList.size());
                    // 写入CSV文件 (使用同步块确保文件写入是线程安全的)
                    synchronized (outputFile) {
                        appendToCsv(outputFile, dataList);
                    }
                    log.info("任务[{}]数据已成功导出到CSV, 行数: {}", taskId, dataList.size());
                } else {
                    log.debug("任务[{}]没有数据需要导出", taskId);
                }
            } catch (Exception e) {
                log.error("写入CSV文件时出错[{}]: {}", taskId, e.getMessage(), e);
            } finally {
                int remaining = pendingExportTasks.decrementAndGet();
                log.debug("任务[{}]完成，剩余{}个导出任务", taskId, remaining);
            }
        }, csvExportExecutor);

        // 添加到任务队列
        csvExportTasks.add(exportTask);
        log.debug("已将任务[{}]添加到导出队列，当前队列任务数: {}", taskId, pendingExportTasks.get());
    }

    /**
     * 初始化CSV文件，写入表头
     */
    private void initCsvFile(File outputFile) throws IOException {
        // 整合后的CSV文件头
        String[] headers = {
            "表名", "所在库",
            "金额字段", "统计项",
            "SUM_ORA_ALL", "SUM_ORA", "SUM_RLCMS_BASE", "SUM_RLCMS_PV1", "SUM_RLCMS_PV2", "SUM_RLCMS_PV3", "SUM_BSCOPY_PV1", "SUM_BSCOPY_PV2", "SUM_BSCOPY_PV3",
            "应用公式", "公式结果", "差异值", "差异描述"
        };

        try (FileOutputStream fos = new FileOutputStream(outputFile);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");
             BufferedWriter writer = new BufferedWriter(osw)) {
            // 写入表头
            writer.write(String.join(",", headers));
            writer.newLine();
        }
        log.info("CSV文件初始化完成，表头已写入: {}", outputFile.getAbsolutePath());
    }

    /**
     * 将数据追加到现有CSV文件
     */
    private void appendToCsv(File outputFile, List<MoneyFieldSumInfo> dataList) throws IOException {
        log.info("开始追加数据到CSV文件: {}", outputFile.getAbsolutePath());

        // 准备CSV数据（不包括表头）
        // 预分配更大空间，避免频繁扩容
        List<String> rows = new ArrayList<>(dataList.size() * 2);

        // 处理每个数据项并生成结果行
        dataList.forEach(info -> {
            // 获取各数据源的值
            String[] dataSources = {"ora", "rlcms_base", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3",
                "bscopy_pv1", "bscopy_pv2", "bscopy_pv3"};

            // 创建基础值列表
            List<String> baseValues = new ArrayList<>(15);
            // 确保表名使用大写
            baseValues.add(escapeCsvValue(info.getTableName().toUpperCase()));
            baseValues.add(escapeCsvValue(info.getDataSources()));
            baseValues.add(escapeCsvValue(info.getMoneyFields()));
            baseValues.add(escapeCsvValue(info.getSumField()));

            // 添加ORA数据源的无WHERE条件SUM值
            String oraAllValue = formatValue(info.getSumValueAllByDataSource("ora"));
            baseValues.add(oraAllValue);

            // 添加各数据源的WHERE条件SUM值
            for (String ds : dataSources) {
                String value = info.isCountField()
                    ? formatValue(info.getCountValueByDataSource(ds))
                    : formatValue(info.getSumValueByDataSource(ds));
                baseValues.add(value);
            }

            // 计算公式结果并添加到行
            List<List<String>> formulaRows = formulaCalculationService.calculateFormulaResults(info, baseValues);
            for (List<String> row : formulaRows) {
                rows.add(String.join(",", row));
            }
        });

        // 将数据追加到文件，使用更大的缓冲区提高性能
        try (FileOutputStream fos = new FileOutputStream(outputFile, true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");
             BufferedWriter writer = new BufferedWriter(osw, 8192)) {

            for (String row : rows) {
                writer.write(row);
                writer.newLine();
            }
            writer.flush();
        }

        log.info("已将{}条记录追加到CSV文件", rows.size());
    }

    /**
     * 格式化数值为CSV字符串
     */
    private String formatValue(Object value) {
        return Optional.ofNullable(value)
            .map(this::formatToString)
            .orElse("");
    }

    /**
     * 将值格式化为字符串表示
     */
    private String formatToString(Object value) {
        return Optional.of(value)
            .map(v -> Optional.of(v)
                .filter(val -> val instanceof BigDecimal)
                .map(val -> (BigDecimal) val)
                .map(bigDecimal -> {
                    // 根据配置的精度进行格式化
                    int scale = dbConfig.getSum().getScale();
                    BigDecimal scaledValue = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
                    return Optional.of(THRESHOLD.compareTo(bigDecimal) < 0)
                        .filter(Boolean::booleanValue)
                        .map(isGreater -> "'" + scaledValue)
                        .orElse(scaledValue.toString());
                })
                .orElseGet(v::toString)
            )
            .orElse("");
    }

    /**
     * 转义CSV值
     */
    private String escapeCsvValue(String value) {
        return Optional.ofNullable(value)
            .map(this::escapeString)
            .orElse("");
    }

    /**
     * 将字符串进行CSV转义处理
     */
    private String escapeString(String value) {
        return (value.contains(",") || value.contains("\"") || value.contains("\n"))
            ? "\"" + value.replace("\"", "\"\"") + "\""
            : value;
    }
}