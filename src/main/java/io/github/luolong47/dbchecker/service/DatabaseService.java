package io.github.luolong47.dbchecker.service;

import cn.hutool.core.date.DateUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import io.github.luolong47.dbchecker.manager.TableInfoManager;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import io.github.luolong47.dbchecker.model.TableInfo;
import io.github.luolong47.dbchecker.util.CsvExportUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
    private final DbConfig whereConditionConfig;
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
        DbConfig whereConditionConfig,
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
        this.whereConditionConfig = whereConditionConfig;
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
        log.debug("配置信息 - 导出目录: {}", whereConditionConfig.getExportDirectory());
        log.debug("配置信息 - 运行模式: {}", whereConditionConfig.getRunMode());
        log.debug("配置信息 - 包含表: {}", whereConditionConfig.getIncludeTables());
        log.debug("配置信息 - 包含Schema: {}", whereConditionConfig.getIncludeSchemas());

        // 根据运行模式初始化状态
        resumeStateManager.initResumeState(whereConditionConfig.getRunMode());
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

        return resumeStateManager.filterDatabases(allDatabases, whereConditionConfig.getRunMode(), whereConditionConfig.getRerunDatabases());
    }

    /**
     * 导出金额字段的SUM值到CSV文件
     */
    public void exportMoneyFieldSumToCsv() throws IOException {
        log.info("当前运行模式: {}", whereConditionConfig.getRunMode());
        if (!cn.hutool.core.util.StrUtil.isEmpty(whereConditionConfig.getRerunDatabases())) {
            log.info("指定重跑数据库: {}", whereConditionConfig.getRerunDatabases());
        }

        // 检查现有状态
        boolean hasRestoredTableInfo = tableInfoManager.hasTableInfo();

        if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(whereConditionConfig.getRunMode())) {
            log.info("已从断点续跑文件恢复{}个表的信息，将继续处理未完成的数据库和表", tableInfoManager.getTableCount());
        } else {
            log.info("开始收集表信息...");
            // 如果没有恢复数据或不是断点续跑模式，则清空已有的表信息
            if (!hasRestoredTableInfo || !"RESUME".equalsIgnoreCase(whereConditionConfig.getRunMode())) {
                tableInfoManager.clearTableInfo();
            }
        }

        // 获取需要处理的数据库
        Map<String, JdbcTemplate> databasesToProcess = getDatabasesToProcess();
        log.info("本次将处理{}个数据库: {}", databasesToProcess.size(), cn.hutool.core.util.StrUtil.join(", ", databasesToProcess.keySet()));

        // 使用CompletableFuture并发获取表信息
        List<CompletableFuture<List<TableInfo>>> futures = new ArrayList<>();

        for (Map.Entry<String, JdbcTemplate> entry : databasesToProcess.entrySet()) {
            String dbName = entry.getKey();
            JdbcTemplate jdbcTemplate = entry.getValue();

            // 如果数据库已经处理过且从断点续跑恢复了数据，则跳过
            if (hasRestoredTableInfo && "RESUME".equalsIgnoreCase(whereConditionConfig.getRunMode())
                && resumeStateManager.isDatabaseProcessed(dbName)) {
                log.info("数据库[{}]已在上次运行中处理，跳过", dbName);
                continue;
            }

            CompletableFuture<List<TableInfo>> future = CompletableFuture.supplyAsync(() -> {
                List<TableInfo> tables = tableMetadataService.getTablesInfoFromDataSource(
                    jdbcTemplate, dbName, whereConditionConfig.getIncludeSchemas(), whereConditionConfig.getIncludeTables(),
                    whereConditionConfig, resumeStateManager, whereConditionConfig.getRunMode());
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
                tableInfoManager.addTableInfoList(tables, dataSourceName);

                // 及时保存状态
                resumeStateManager.saveResumeState(tableInfoManager.getTableInfoMap());
            } catch (Exception e) {
                log.error("获取表信息时出错: {}", e.getMessage(), e);
            }
        }

        log.info("表信息收集完成，共发现{}个表", tableInfoManager.getTableCount());

        // 创建结果目录
        File directory = new File(whereConditionConfig.getExportDirectory());
        if (!directory.exists()) {
            cn.hutool.core.io.FileUtil.mkdir(directory);
        }

        // 创建输出文件
        File outputFile = cn.hutool.core.io.FileUtil.file(directory,
            cn.hutool.core.util.StrUtil.format("表金额字段SUM比对-{}.csv", DateUtil.format(DateUtil.date(), "yyyyMMdd-HHmmss")));

        // 输出到CSV
        List<MoneyFieldSumInfo> dataList = formulaCalculationService.prepareExportData(tableInfoManager.getTableInfoMap());
        CsvExportUtil.exportToCsv(outputFile, dataList, formulaCalculationService);

        // 在最后，确保所有处理都被标记为已完成
        resumeStateManager.saveResumeState(tableInfoManager.getTableInfoMap());
    }
}