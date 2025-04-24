package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import cn.hutool.core.text.csv.CsvUtil;
import cn.hutool.core.text.csv.CsvWriteConfig;
import cn.hutool.core.text.csv.CsvWriter;
import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.Dbconfig;
import io.github.luolong47.dbchecker.entity.TableCsvResult;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CSV导出管理器，负责处理数据导出到CSV文件的相关功能
 */
@Slf4j
@Data
@Component
public class CsvExportManager {
    // CSV导出相关常量
    public static final BigDecimal THRESHOLD = new BigDecimal("999999999999999");
    public static final String[] HEADERS = new String[]{
        "表名", "所在库", "金额字段", "统计项", "SUM_ORA_ALL", "SUM_ORA",
        "SUM_RLCMS_BASE", "SUM_RLCMS_PV1", "SUM_RLCMS_PV2", "SUM_RLCMS_PV3",
        "SUM_BSCOPY_PV1", "SUM_BSCOPY_PV2", "SUM_BSCOPY_PV3",
        "应用公式", "公式结果", "差异值", "差异描述"
    };

    // 添加计数器，用于显示进度
    private AtomicInteger csvExportCounter = new AtomicInteger(0);
    // 添加共享文件锁，确保多线程写入安全
    private final Object csvFileLock = new Object();
    // 添加共享的CSV写入器
    private CsvWriter csvWriter;
    // 保存CSV文件路径
    private File csvFile;
    
    private final Dbconfig dbconfig;

    public CsvExportManager(Dbconfig dbconfig) {
        this.dbconfig = dbconfig;
    }

    /**
     * 初始化CSV导出
     * 
     * @param totalTables 总表数量，用于进度计算
     */
    public void initCsvExport(int totalTables) {
        try {
            // 重置计数器
            csvExportCounter.set(0);
            log.info("共需处理 {} 张表的数据导出", totalTables);
            
            // 获取导出目录
            String exportDir = Optional.ofNullable(dbconfig.getExport())
                .map(Dbconfig.Export::getDirectory)
                .orElse("./export");

            // 构建CSV文件路径 - 使用当前时间戳作为文件名
            String filePath = StrUtil.format("db_checker_result_{}.csv", new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
            csvFile = new File(exportDir, filePath);
            FileUtil.mkParentDirs(csvFile);

            log.info("初始化CSV导出，文件路径: {}", csvFile);

            // 创建CSV写入器 - 配置为支持Bean写入，指定头部别名
            CsvWriteConfig config = new CsvWriteConfig();
            config.setHeaderAlias(createHeaderAlias()); // 设置表头别名映射

            // 使用FileWriter创建CsvWriter
            FileWriter fileWriter = new FileWriter(csvFile);

            // 创建CsvWriter，并初始化表头
            csvWriter = CsvUtil.getWriter(fileWriter, config);

            // 手动写入表头行，原来的writeHeaderLine()可能没有生效
            csvWriter.write(HEADERS);

            log.info("CSV写入器初始化完成，已手动写入表头");
        } catch (Exception e) {
            log.error("初始化CSV导出失败: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 导出表数据到CSV
     * 
     * @param tableName 表名
     * @param results 表的CSV结果列表
     * @param totalTables 总表数量，用于进度计算
     */
    public void exportTableToCsv(String tableName, List<TableCsvResult> results, int totalTables) {
        if (results == null || results.isEmpty()) {
            log.warn("表 [{}] 没有有效的求和结果，跳过CSV导出", tableName);
            return;
        }
        
        // 线程安全地将结果写入CSV
        synchronized (csvFileLock) {
            if (csvWriter != null) {
                try {
                    // 将所有参数转换为字符串，放入数组中传递给write方法
                    for (TableCsvResult result : results) {
                        // 将每个对象的字段值转换为字符串数组
                        String[] rowData = new String[]{
                            result.getTableName(),
                            result.getDbs(),
                            result.getSumCols(),
                            result.getCol(),
                            formatBigDecimal(result.getSumOraAll()),
                            formatBigDecimal(result.getSumOra()),
                            formatBigDecimal(result.getSumRlcmsBase()),
                            formatBigDecimal(result.getSumRlcmsPv1()),
                            formatBigDecimal(result.getSumRlcmsPv2()),
                            formatBigDecimal(result.getSumRlcmsPv3()),
                            formatBigDecimal(result.getSumBscopyPv1()),
                            formatBigDecimal(result.getSumBscopyPv2()),
                            formatBigDecimal(result.getSumBscopyPv3()),
                            result.getFormula() != null ? result.getFormula() : "",
                            result.getFormulaResult() != null ? result.getFormulaResult() : "",
                            formatBigDecimal(result.getDiff()),
                            result.getDiffDesc() != null ? result.getDiffDesc() : ""
                        };
                        csvWriter.write(rowData);
                    }
                    log.debug("表 [{}] 的 {} 条记录已写入CSV", tableName, results.size());
                } catch (Exception e) {
                    log.error("写入表 [{}] 的CSV数据时发生错误: {}", tableName, e.getMessage(), e);
                }
            } else {
                log.warn("CSV写入器为空，无法写入表 [{}] 的数据", tableName);
            }
        }
        
        // 更新计数器并显示进度
        int current = csvExportCounter.incrementAndGet();
        log.info("数据处理进度: 第 {}/{} 张表 ({}%) - 表 [{}] 的数据处理已完成",
            current, totalTables, Math.round((float) current / totalTables * 100), tableName);
    }
    
    /**
     * 关闭CSV写入器
     */
    public void closeWriter() {
        IoUtil.close(csvWriter);
        log.info("CSV写入器已关闭，文件保存在: {}", csvFile);
    }

    /**
     * 创建CSV表头与Bean字段的映射关系
     *
     * @return 表头别名映射
     */
    private Map<String, String> createHeaderAlias() {
        Map<String, String> headerAlias = new HashMap<>();
        headerAlias.put("tableName", "表名");
        headerAlias.put("dbs", "所在库");
        headerAlias.put("sumCols", "金额字段");
        headerAlias.put("col", "统计项");
        headerAlias.put("sumOraAll", "SUM_ORA_ALL");
        headerAlias.put("sumOra", "SUM_ORA");
        headerAlias.put("sumRlcmsBase", "SUM_RLCMS_BASE");
        headerAlias.put("sumRlcmsPv1", "SUM_RLCMS_PV1");
        headerAlias.put("sumRlcmsPv2", "SUM_RLCMS_PV2");
        headerAlias.put("sumRlcmsPv3", "SUM_RLCMS_PV3");
        headerAlias.put("sumBscopyPv1", "SUM_BSCOPY_PV1");
        headerAlias.put("sumBscopyPv2", "SUM_BSCOPY_PV2");
        headerAlias.put("sumBscopyPv3", "SUM_BSCOPY_PV3");
        headerAlias.put("formula", "应用公式");
        headerAlias.put("formulaResult", "公式结果");
        headerAlias.put("diff", "差异值");
        headerAlias.put("diffDesc", "差异描述");
        return headerAlias;
    }

    /**
     * 格式化BigDecimal值，当值大于999999999999999时，添加单引号前缀
     * 
     * @param value BigDecimal值
     * @return 格式化后的字符串
     */
    private String formatBigDecimal(BigDecimal value) {
        if (value == null) {
            return "";
        }
        
        // 使用compareTo进行比较，确保数值比较的准确性
        if (value.compareTo(THRESHOLD) > 0) {
            // 添加单引号前缀
            return StrUtil.format("'{}", value);
        } else {
            return value.toString();
        }
    }
} 
