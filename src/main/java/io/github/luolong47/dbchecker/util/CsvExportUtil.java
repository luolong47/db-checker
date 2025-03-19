package io.github.luolong47.dbchecker.util;

import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import io.github.luolong47.dbchecker.service.FormulaCalculationService;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * CSV导出工具类
 */
@Slf4j
public class CsvExportUtil {

    /**
     * 导出数据到CSV文件
     */
    public static void exportToCsv(File outputFile, List<MoneyFieldSumInfo> dataList,
                                   FormulaCalculationService formulaCalculationService) throws IOException {
        log.info("开始导出CSV: {}", outputFile.getAbsolutePath());
        log.info("总数据量: {} 条记录", dataList.size());

        // 准备CSV数据
        List<List<String>> csvRows = prepareExportData(dataList, formulaCalculationService);

        // 写入文件
        writeToCsvFile(outputFile, csvRows);

        log.info("CSV导出完成: {}", outputFile.getAbsolutePath());
    }

    /**
     * 准备CSV导出数据
     */
    private static List<List<String>> prepareExportData(List<MoneyFieldSumInfo> dataList,
                                                        FormulaCalculationService formulaCalculationService) {
        // 整合后的CSV文件头
        String[] headers = {
            "表名", "所在库",
            "金额字段", "统计项",
            "SUM_ORA", "SUM_RLCMS_BASE", "SUM_RLCMS_PV1", "SUM_RLCMS_PV2", "SUM_RLCMS_PV3", "SUM_BSCOPY_PV1", "SUM_BSCOPY_PV2", "SUM_BSCOPY_PV3",
            "应用公式", "公式结果", "差异值", "差异描述"
        };

        // 创建结果集合，先添加表头
        List<List<String>> rows = new ArrayList<>();
        rows.add(Arrays.asList(headers));

        // 使用Stream API处理每个数据项并生成结果行
        List<List<String>> dataRows = dataList.stream()
            .flatMap(info -> {
                // 获取各数据源的值
                String[] dataSources = {"ora", "rlcms_base", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3",
                    "bscopy_pv1", "bscopy_pv2", "bscopy_pv3"};

                // 创建基础值列表
                List<String> baseValues = new ArrayList<>();
                baseValues.add(escapeCsvValue(info.getTableName()));
                baseValues.add(escapeCsvValue(info.getDataSources()));
                baseValues.add(escapeCsvValue(info.getMoneyFields()));
                baseValues.add(escapeCsvValue(info.getSumField()));

                // 添加各数据源的值
                Arrays.stream(dataSources).forEach(ds -> {
                    String value = info.isCountField()
                        ? formatValue(info.getCountValueByDataSource(ds))
                        : formatValue(info.getSumValueByDataSource(ds));
                    baseValues.add(value);
                });

                // 计算公式结果并返回多行
                List<List<String>> formulaRows = formulaCalculationService.calculateFormulaResults(info, baseValues);
                return formulaRows.stream();
            })
            .collect(Collectors.toList());

        // 合并表头和数据行
        rows.addAll(dataRows);
        return rows;
    }

    /**
     * 格式化数值为CSV字符串
     */
    private static String formatValue(Object value) {
        return Optional.ofNullable(value)
            .map(CsvExportUtil::formatToString)
            .orElse("");
    }

    /**
     * 将值格式化为字符串表示
     */
    private static String formatToString(Object value) {
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
    private static String escapeCsvValue(String value) {
        return Optional.ofNullable(value)
            .map(CsvExportUtil::escapeString)
            .orElse("");
    }

    /**
     * 将字符串进行CSV转义处理
     */
    private static String escapeString(String value) {
        return (value.contains(",") || value.contains("\"") || value.contains("\n"))
            ? "\"" + value.replace("\"", "\"\"") + "\""
            : value;
    }

    /**
     * 将数据写入CSV文件
     */
    private static void writeToCsvFile(File outputFile, List<List<String>> rows) throws IOException {
        log.info("开始写入CSV文件: {}", outputFile.getAbsolutePath());
        int totalSize = rows.size() - 1; // 减去表头

        try (FileOutputStream fos = new FileOutputStream(outputFile);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "GBK");
             BufferedWriter writer = new BufferedWriter(osw)) {

            // 提取表头单独处理
            List<String> header = rows.get(0);
            writer.write(String.join(",", header));
            writer.newLine();

            // 使用Stream处理数据行
            IntStream.range(1, rows.size())
                .mapToObj(rows::get)
                .forEachOrdered(row -> {
                    try {
                        writer.write(String.join(",", row));
                        writer.newLine();

                        // 每1000行记录一次日志
                        int index = rows.indexOf(row);
                        if (index % 1000 == 0) {
                            log.info("已处理 {}/{} 行 ({}%)",
                                index, totalSize,
                                Math.round((index * 100.0) / totalSize));
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException("写入CSV行时出错", e);
                    }
                });
        } catch (UncheckedIOException e) {
            throw new IOException("写入CSV文件失败", e.getCause());
        }

        log.info("CSV文件写入完成: {}", outputFile.getAbsolutePath());
    }
} 