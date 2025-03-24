package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import io.github.luolong47.dbchecker.manager.TableInfoManager;
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

/**
 * 数据库服务类，用于测试多数据源
 */
@Slf4j
@Service
public class DatabaseService {

    private final JdbcTemplate oraJdbcTemplate;
    private final JdbcTemplate oraSlaveJdbcTemplate;
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
        TableMetadataService tableMetadataService) {
        this.oraJdbcTemplate = oraJdbcTemplate;
        this.oraSlaveJdbcTemplate = oraSlaveJdbcTemplate;
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

        // 用于追踪已完成处理的表
        Set<String> processedTables = Collections.synchronizedSet(new HashSet<>());
        
        // 使用CompletableFuture并发获取表信息
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, JdbcTemplate> entry : databasesToProcess.entrySet()) {
            String dbName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();

            // 如果数据库已经处理过且从断点续跑恢复了数据，则跳过
            if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(dbConfig.getRunMode())
                && resumeStateManager.isDatabaseProcessed(dbName)) {
                log.info("数据库[{}]已在上次运行中处理，跳过", dbName);
                continue;
            }

            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                List<TableInfo> tables = tableMetadataService.getTablesInfoFromDataSource(
                    jdbcTemplate, dbName, dbConfig.getIncludeSchemas(), dbConfig.getIncludeTables(),
                    dbConfig, resumeStateManager, dbConfig.getRunMode());
                log.info("从{}数据源获取到{}个表", dbName, tables.size());
                return tables;
            }).thenAccept(tables -> {
                try {
                    if (tables.isEmpty()) {
                        return;
                    }

                    TableInfo firstTable = tables.get(0);
                    String dataSourceName = firstTable.getDataSourceName();

                    // 添加表信息到映射
                    tableInfoManager.addTableInfoList(tables, dataSourceName);

                    // 对于每张表，检查是否已收集完所有数据源的数据，并尝试导出
                    for (TableInfo table : tables) {
                        synchronized (processedTables) {
                            checkAndExportTable(table.getTableName(), processedTables, outputFile);
                        }
                    }

                    // 及时保存状态
                    resumeStateManager.saveResumeState(tableInfoManager.getTableInfoMap());
                } catch (Exception e) {
                    log.error("处理表信息时出错: {}", e.getMessage(), e);
                }
            });

            futures.add(future);
        }

        // 等待所有处理完成
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("表信息收集完成，共发现{}个表", tableInfoManager.getTableCount());

        // 导出所有尚未导出的表
        processRemainingTables(processedTables, outputFile);

        // 在最后，确保所有处理都被标记为已完成
        resumeStateManager.saveResumeState(tableInfoManager.getTableInfoMap());
    }

    /**
     * 检查并导出单个表
     */
    private void checkAndExportTable(String tableName, Set<String> processedTables, File outputFile) throws IOException {
        // 如果表已经处理过，跳过
        if (processedTables.contains(tableName)) {
            return;
        }

        // 获取表信息
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

        // 如果所有必需的数据源都已收集，则导出
        if (collectedDataSources.containsAll(requiredDataSources)) {
            log.info("表[{}]已收集完所有必需的数据源数据，准备导出", tableName);

            // 创建只包含此表的Map
            Map<String, TableInfo> singleTableMap = new HashMap<>();
            singleTableMap.put(tableName, tableInfo);

            // 生成导出数据并追加到CSV文件
            List<MoneyFieldSumInfo> dataList = formulaCalculationService.prepareExportData(singleTableMap);

            // 使用同步块确保CSV文件写入是线程安全的
            synchronized (outputFile) {
                appendToCsv(outputFile, dataList);
            }

            // 标记为已处理
            processedTables.add(tableName);

            log.info("表[{}]数据已成功导出到CSV", tableName);
        }
    }

    /**
     * 处理所有剩余尚未导出的表
     */
    private void processRemainingTables(Set<String> processedTables, File outputFile) throws IOException {
        Map<String, TableInfo> tableInfoMap = tableInfoManager.getTableInfoMap();

        // 筛选出尚未处理的表
        Map<String, TableInfo> remainingTables = new HashMap<>();
        synchronized (processedTables) {
            for (Map.Entry<String, TableInfo> entry : tableInfoMap.entrySet()) {
                if (!processedTables.contains(entry.getKey())) {
                    remainingTables.put(entry.getKey(), entry.getValue());
                }
            }
        }

        if (!remainingTables.isEmpty()) {
            log.info("发现{}个表尚未导出，开始批量导出", remainingTables.size());

            // 导出剩余表
            List<MoneyFieldSumInfo> dataList = formulaCalculationService.prepareExportData(remainingTables);

            // 使用同步块确保CSV文件写入是线程安全的
            synchronized (outputFile) {
                appendToCsv(outputFile, dataList);
            }

            log.info("已完成{}个剩余表的数据导出", remainingTables.size());
        }
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
        List<List<String>> rows = new ArrayList<>();

        // 使用Stream API处理每个数据项并生成结果行
        dataList.forEach(info -> {
            // 获取各数据源的值
            String[] dataSources = {"ora", "rlcms_base", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3",
                "bscopy_pv1", "bscopy_pv2", "bscopy_pv3"};

            // 创建基础值列表
            List<String> baseValues = new ArrayList<>();
            baseValues.add(escapeCsvValue(info.getTableName()));
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
            rows.addAll(formulaRows);
        });

        // 将数据追加到文件
        try (FileOutputStream fos = new FileOutputStream(outputFile, true);  // 注意这里设置为append模式
             OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");
             BufferedWriter writer = new BufferedWriter(osw)) {

            for (List<String> row : rows) {
                writer.write(String.join(",", row));
                writer.newLine();
            }
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
        if (value instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) value;
            // 对值取100000000000000的余
            BigDecimal remainder = bigDecimal.remainder(new BigDecimal("100000000000000"));
            // 保持3位小数
            return remainder.setScale(3, RoundingMode.HALF_UP).toString();
        }
        return value.toString();
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