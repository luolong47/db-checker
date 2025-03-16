package io.github.luolong47.dbchecker.service;

import cn.hutool.core.util.StrUtil;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.model.MoneyFieldSumInfo;
import io.github.luolong47.dbchecker.model.TableInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 公式计算服务
 */
@Slf4j
@Service
public class FormulaCalculationService {

    // 公式描述常量
    private static final Map<Integer, String> FORMULA_DESCRIPTIONS = new HashMap<>();

    static {
        // 初始化公式描述
        FORMULA_DESCRIPTIONS.put(1, "（分省-拆分）: ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3");
        FORMULA_DESCRIPTIONS.put(2, "（仅基础）: ora = rlcms_base");
        FORMULA_DESCRIPTIONS.put(3, "（基础+副本）: ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3");
        FORMULA_DESCRIPTIONS.put(4, "（分省-冗余）: ora = rlcms_pv1 = rlcms_pv2 = rlcms_pv3");
        FORMULA_DESCRIPTIONS.put(5, "（全部）: ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3");
        FORMULA_DESCRIPTIONS.put(6, "（分省1）: ora = rlcms_pv1");
    }

    // 存储各公式适用的表名集合
    private final Map<Integer, Set<String>> formulaTableMap = new HashMap<>();
    // 存储公式策略的映射
    private final Map<Integer, FormulaStrategy> formulaStrategies = new HashMap<>();
    private final Map<Integer, CountFormulaStrategy> countStrategies = new HashMap<>();

    // 配置类
    private final DbConfig config;

    public FormulaCalculationService(DbConfig config) {
        this.config = config;
        // 初始化公式策略
        initFormulaStrategies();
    }

    /**
     * 初始化公式适用表集合
     */
    public void initFormulaTableMap() {
        // 使用Stream API统一处理所有公式配置
        Map<Integer, String> formulaConfigs = new HashMap<>();
        formulaConfigs.put(1, config.getFormula1());
        formulaConfigs.put(2, config.getFormula2());
        formulaConfigs.put(3, config.getFormula3());
        formulaConfigs.put(4, config.getFormula4());
        formulaConfigs.put(5, config.getFormula5());
        formulaConfigs.put(6, config.getFormula6());

        // 记录每个表已经应用的公式
        Map<String, Integer> tableFormulaMap = new HashMap<>();
        List<String> duplicateTables = new ArrayList<>();

        // 一次性初始化所有公式的适用表集合
        for (Map.Entry<Integer, String> entry : formulaConfigs.entrySet()) {
            Integer formulaNum = entry.getKey();
            String tablesStr = entry.getValue();

            if (StrUtil.isBlank(tablesStr)) {
                formulaTableMap.put(formulaNum, new HashSet<>());
                continue;
            }

            // 解析当前公式的表集合
            Set<String> tableSet = Arrays.stream(tablesStr.split(","))
                .map(String::trim)
                .collect(Collectors.toSet());

            // 检查每个表是否已应用其他公式
            for (String table : tableSet) {
                if (tableFormulaMap.containsKey(table)) {
                    Integer existingFormula = tableFormulaMap.get(table);
                    duplicateTables.add(String.format("表[%s]同时应用于公式%d和公式%d",
                        table, existingFormula, formulaNum));
                } else {
                    tableFormulaMap.put(table, formulaNum);
                }
            }

            // 保存当前公式的表集合
            formulaTableMap.put(formulaNum, tableSet);
            log.info("公式{}适用表: {}", formulaNum, StrUtil.join(", ", tableSet));
        }

        // 如果有表出现在多个公式中，则报错并退出
        if (!duplicateTables.isEmpty()) {
            String errorMsg = String.format("发现%d个表被应用到多个公式中:\n%s",
                duplicateTables.size(),
                duplicateTables.stream().collect(Collectors.joining("\n")));
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
    }

    /**
     * 准备导出数据
     */
    public List<MoneyFieldSumInfo> prepareExportData(Map<String, TableInfo> tableInfoMap) {
        // 对表名进行字母排序
        List<String> sortedKeys = new ArrayList<>(tableInfoMap.keySet());
        Collections.sort(sortedKeys);

        log.info("开始计算各表金额字段SUM值...");

        // 创建一个临时Map用于按表名分组
        Map<String, List<MoneyFieldSumInfo>> tableNameGroupMap = new HashMap<>();

        // 使用TableMetaInfo中的求和结果，直接构建输出信息
        sortedKeys.forEach(key -> {
            TableInfo metaInfo = tableInfoMap.get(key);

            // 对金额字段按字母排序
            List<String> sortedMoneyFields = new ArrayList<>(metaInfo.getMoneyFields());
            Collections.sort(sortedMoneyFields);

            // 如果没有金额字段，不再创建额外的记录，COUNT记录将在exportToCsv方法中添加
            if (!sortedMoneyFields.isEmpty()) {
                // 为每个金额字段创建MoneyFieldSumInfo并添加到对应表名的分组中
                sortedMoneyFields.forEach(moneyField -> {
                    MoneyFieldSumInfo sumInfo = new MoneyFieldSumInfo(
                        key,
                        metaInfo,
                        moneyField
                    );
                    // 添加到表名分组
                    tableNameGroupMap.computeIfAbsent(key, k -> new ArrayList<>()).add(sumInfo);
                });
            } else {
                // 对于没有金额字段的表，确保表名在映射中有一个空列表
                // 这样在exportToCsv中仍然能为其创建COUNT特殊字段
                tableNameGroupMap.computeIfAbsent(key, k -> new ArrayList<>());
            }
        });

        // 收集所有表名
        Set<String> allTables = tableInfoMap.keySet();

        // 收集所有来自tableInfoMap的表名，包括没有金额字段的表
        Set<String> allTablesFromMetaInfo = new HashSet<>(tableInfoMap.keySet());

        // 日志输出分析
        log.info("数据列表中的表数量: {}, 元数据中的表数量: {}",
            allTables.size(), allTablesFromMetaInfo.size());

        // 找出在元数据中存在但不在数据列表中的表（没有金额字段的表）
        Set<String> tablesWithoutMoneyFields = tableNameGroupMap.entrySet().stream()
            .filter(entry -> entry.getValue().isEmpty())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());

        // 记录日志
        if (tablesWithoutMoneyFields.isEmpty()) {
            log.info("所有表都有金额字段，无需特殊处理");
        } else {
            log.info("发现{}个没有金额字段的表: {}",
                tablesWithoutMoneyFields.size(),
                StrUtil.join(", ", tablesWithoutMoneyFields.stream().sorted().limit(10).collect(Collectors.toList())) +
                    (tablesWithoutMoneyFields.size() > 10 ? "..." : ""));
        }

        // 创建富集数据列表
        List<MoneyFieldSumInfo> enrichedDataList = createEnrichedDataList(tableNameGroupMap, tablesWithoutMoneyFields, tableInfoMap);

        // 统计信息
        int tablesWithFieldsCount = tableNameGroupMap.size() - tablesWithoutMoneyFields.size();
        int tablesWithoutFieldsCount = tablesWithoutMoneyFields.size();
        int totalTablesCount = tablesWithFieldsCount + tablesWithoutFieldsCount;

        log.info("最终处理表统计：共{}个表，其中{}个有金额字段表，{}个无金额字段表",
            totalTablesCount, tablesWithFieldsCount, tablesWithoutFieldsCount);
        log.info("最终输出数据行数: {}", enrichedDataList.size());

        return enrichedDataList;
    }

    /**
     * 创建富集的数据列表（包含COUNT特殊字段）
     */
    private List<MoneyFieldSumInfo> createEnrichedDataList(
        Map<String, List<MoneyFieldSumInfo>> tableGroups,
        Set<String> tablesWithoutMoneyFields,
        Map<String, TableInfo> tableInfoMap) {

        // 最终结果列表
        List<MoneyFieldSumInfo> enrichedDataList = new ArrayList<>();

        // 用于跟踪已处理的表，避免重复添加COUNT字段
        Set<String> processedCountTables = new HashSet<>();

        // 1. 处理有金额字段的表 - 首先添加COUNT特殊字段，然后添加所有金额字段
        tableGroups.entrySet().stream()
            .filter(entry -> !entry.getValue().isEmpty()) // 过滤掉没有金额字段的表
            .forEach(entry -> {
                String tableName = entry.getKey();
                List<MoneyFieldSumInfo> tableData = entry.getValue();

                // 对于每个表，只添加一次COUNT特殊字段
                if (!processedCountTables.contains(tableName)) {
                    // 添加COUNT特殊字段
                    Optional.ofNullable(tableInfoMap.get(tableName))
                        .map(info -> new MoneyFieldSumInfo(tableName, info, "_COUNT"))
                        .ifPresent(enrichedDataList::add);
                    processedCountTables.add(tableName);
                }

                // 添加所有金额字段SUM信息
                enrichedDataList.addAll(tableData);
            });

        // 2. 处理没有金额字段的表 - 仅添加COUNT特殊字段
        tablesWithoutMoneyFields.stream()
            .filter(tableName -> !processedCountTables.contains(tableName)) // 避免重复添加
            .map(tableInfoMap::get)
            .filter(Objects::nonNull)
            .map(info -> {
                MoneyFieldSumInfo countInfo = new MoneyFieldSumInfo(info.getTableName(), info, "_COUNT");
                log.debug("为没有金额字段的表[{}]添加COUNT特殊字段", info.getTableName());
                return countInfo;
            })
            .forEach(enrichedDataList::add);

        // 按表名和字段名排序
        return enrichedDataList.stream()
            .sorted(Comparator.comparing(MoneyFieldSumInfo::getTableName)
                .thenComparing(MoneyFieldSumInfo::getSumField))
            .collect(Collectors.toList());
    }

    /**
     * 计算表的公式结果并创建多行结果
     *
     * @param info       表信息
     * @param baseValues 基础值列表（表名、所在库等）
     * @return 包含多行结果的列表，每行代表一个适用的公式结果
     */
    public List<List<String>> calculateFormulaResults(MoneyFieldSumInfo info, List<String> baseValues) {
        List<List<String>> results = new ArrayList<>();
        String tableName = info.getTableName();

        // 获取适用于该表的公式
        List<String> applicableFormulas = getApplicableFormulas(tableName);

        if (applicableFormulas.isEmpty()) {
            // 如果没有适用的公式，仍然输出一行，但公式和结果为空
            List<String> row = new ArrayList<>(baseValues);
            row.add("");
            row.add("");
            results.add(row);
            return results;
        }

        // 为每个适用的公式创建一行结果
        for (String formula : applicableFormulas) {
            List<String> row = new ArrayList<>(baseValues);
            // 从公式字符串中提取编号，跳过"公式X："前缀
            int formulaNumber = Integer.parseInt(formula.substring(2, formula.indexOf("：")));

            // 根据字段类型选择不同的公式策略
            String result;
            if (info.isCountField()) {
                // 对于记录数统计字段，使用计数公式策略
                result = countStrategies.get(formulaNumber).calculate(info);
            } else {
                // 对于金额字段，使用求和公式策略
                result = formulaStrategies.get(formulaNumber).calculateForSum(info);
            }

            row.add(formula);
            row.add(result);
            results.add(row);
        }

        return results;
    }

    /**
     * 获取应用于指定表的公式名称列表（带描述）
     */
    public List<String> getApplicableFormulas(String tableName) {
        List<String> formulas = new ArrayList<>();

        for (int i = 1; i <= 6; i++) {
            if (shouldApplyFormula(tableName, i)) {
                String formulaDesc = FORMULA_DESCRIPTIONS.getOrDefault(i, "");
                formulas.add("公式" + i + "：" + formulaDesc);
            }
        }

        return formulas;
    }

    /**
     * 检查表是否应当应用特定公式
     */
    public boolean shouldApplyFormula(String tableName, int formulaNumber) {
        // 如果表名有引号，去掉引号
        String normalizedTableName = tableName.startsWith("\"") && tableName.endsWith("\"")
            ? tableName.substring(1, tableName.length() - 1)
            : tableName;

        // 获取适用表集合，如果为空则返回false
        return Optional.ofNullable(formulaTableMap.get(formulaNumber))
            .filter(tables -> !tables.isEmpty())
            .map(tables ->
                tables.stream()
                    .map(String::trim)
                    .anyMatch(configTable -> configTable.equalsIgnoreCase(normalizedTableName.trim()))
            )
            .orElse(false);
    }

    /**
     * 初始化公式策略
     */
    private void initFormulaStrategies() {
        // 定义公式需要的数据源映射
        Map<Integer, String[]> formulaDataSources = new HashMap<>();
        formulaDataSources.put(1, new String[]{"ora", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3"});
        formulaDataSources.put(2, new String[]{"ora", "rlcms_base"});
        formulaDataSources.put(3, new String[]{"ora", "rlcms_base", "bscopy_pv1", "bscopy_pv2", "bscopy_pv3"});
        formulaDataSources.put(4, new String[]{"ora", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3"});
        formulaDataSources.put(5, new String[]{"ora", "rlcms_base", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3"});
        formulaDataSources.put(6, new String[]{"ora", "rlcms_pv1"});

        // 公式1: ora = rlcms_pv1 + rlcms_pv2 + rlcms_pv3
        formulaStrategies.put(1, info -> {
            Map<String, BigDecimal> values = collectSumValues(info, formulaDataSources.get(1));
            if (containsNull(values)) return "N/A";

            BigDecimal sum = values.get("rlcms_pv1").add(values.get("rlcms_pv2")).add(values.get("rlcms_pv3"));
            return isApproximatelyEqual(values.get("ora"), sum) ? "TRUE" : "FALSE";
        });

        countStrategies.put(1, info -> {
            Map<String, Long> values = collectCountValues(info, formulaDataSources.get(1));
            if (containsNull(values)) return "N/A";

            long sum = values.get("rlcms_pv1") + values.get("rlcms_pv2") + values.get("rlcms_pv3");
            return values.get("ora").equals(sum) ? "TRUE" : "FALSE";
        });

        // 公式2: ora = rlcms_base
        formulaStrategies.put(2, info -> {
            Map<String, BigDecimal> values = collectSumValues(info, formulaDataSources.get(2));
            if (containsNull(values)) return "N/A";

            return isApproximatelyEqual(values.get("ora"), values.get("rlcms_base")) ? "TRUE" : "FALSE";
        });

        countStrategies.put(2, info -> {
            Map<String, Long> values = collectCountValues(info, formulaDataSources.get(2));
            if (containsNull(values)) return "N/A";

            return values.get("ora").equals(values.get("rlcms_base")) ? "TRUE" : "FALSE";
        });

        // 公式3: ora = rlcms_base = bscopy_pv1 = bscopy_pv2 = bscopy_pv3
        formulaStrategies.put(3, info -> {
            Map<String, BigDecimal> values = collectSumValues(info, formulaDataSources.get(3));
            if (containsNull(values)) return "N/A";

            return areAllValuesEqual(values.values().toArray(new BigDecimal[0])) ? "TRUE" : "FALSE";
        });

        countStrategies.put(3, info -> {
            Map<String, Long> values = collectCountValues(info, formulaDataSources.get(3));
            if (containsNull(values)) return "N/A";

            return areAllCountValuesEqual(values.values()) ? "TRUE" : "FALSE";
        });

        // 公式4: ora = rlcms_pv1 = rlcms_pv2 = rlcms_pv3
        formulaStrategies.put(4, info -> {
            Map<String, BigDecimal> values = collectSumValues(info, formulaDataSources.get(4));
            if (containsNull(values)) return "N/A";

            return areAllValuesEqual(values.values().toArray(new BigDecimal[0])) ? "TRUE" : "FALSE";
        });

        countStrategies.put(4, info -> {
            Map<String, Long> values = collectCountValues(info, formulaDataSources.get(4));
            if (containsNull(values)) return "N/A";

            return areAllCountValuesEqual(values.values()) ? "TRUE" : "FALSE";
        });

        // 公式5: ora = rlcms_base = rlcms_pv1 = rlcms_pv2 = rlcms_pv3
        formulaStrategies.put(5, info -> {
            Map<String, BigDecimal> values = collectSumValues(info, formulaDataSources.get(5));
            if (containsNull(values)) return "N/A";

            return areAllValuesEqual(values.values().toArray(new BigDecimal[0])) ? "TRUE" : "FALSE";
        });

        countStrategies.put(5, info -> {
            Map<String, Long> values = collectCountValues(info, formulaDataSources.get(5));
            if (containsNull(values)) return "N/A";

            return areAllCountValuesEqual(values.values()) ? "TRUE" : "FALSE";
        });

        // 公式6: ora = rlcms_pv1
        formulaStrategies.put(6, info -> {
            Map<String, BigDecimal> values = collectSumValues(info, formulaDataSources.get(6));
            if (containsNull(values)) return "N/A";

            return isApproximatelyEqual(values.get("ora"), values.get("rlcms_pv1")) ? "TRUE" : "FALSE";
        });

        countStrategies.put(6, info -> {
            Map<String, Long> values = collectCountValues(info, formulaDataSources.get(6));
            if (containsNull(values)) return "N/A";

            return values.get("ora").equals(values.get("rlcms_pv1")) ? "TRUE" : "FALSE";
        });
    }

    /**
     * 收集各数据源的SUM值
     */
    private Map<String, BigDecimal> collectSumValues(MoneyFieldSumInfo info, String[] dataSources) {
        Map<String, BigDecimal> values = new HashMap<>();
        for (String ds : dataSources) {
            values.put(ds, info.getSumValueByDataSource(ds));
        }
        return values;
    }

    /**
     * 收集各数据源的COUNT值
     */
    private Map<String, Long> collectCountValues(MoneyFieldSumInfo info, String[] dataSources) {
        Map<String, Long> values = new HashMap<>();
        for (String ds : dataSources) {
            values.put(ds, info.getCountValueByDataSource(ds));
        }
        return values;
    }

    /**
     * 检查Map中是否包含null值
     */
    private <T> boolean containsNull(Map<String, T> map) {
        return map.values().stream().anyMatch(Objects::isNull);
    }

    /**
     * 检查所有BigDecimal值是否近似相等
     */
    private boolean areAllValuesEqual(BigDecimal... values) {
        if (values.length <= 1) {
            return true;
        }

        BigDecimal reference = values[0];
        return Arrays.stream(values)
            .skip(1)
            .allMatch(value -> isApproximatelyEqual(reference, value));
    }

    /**
     * 检查所有COUNT值是否相等
     */
    private boolean areAllCountValuesEqual(Collection<Long> values) {
        return values.stream().distinct().count() <= 1;
    }

    /**
     * 检查两个BigDecimal是否近似相等（差值小于0.01）
     */
    private boolean isApproximatelyEqual(BigDecimal a, BigDecimal b) {
        return Optional.ofNullable(a)
            .flatMap(valueA -> Optional.ofNullable(b)
                .map(valueB -> valueA.subtract(valueB).abs().compareTo(new BigDecimal("0.01")) <= 0))
            .orElse(false);
    }

    /**
     * 定义公式计算策略接口
     */
    @FunctionalInterface
    public interface FormulaStrategy {
        String calculateForSum(MoneyFieldSumInfo info);
    }

    /**
     * 定义COUNT公式计算策略接口
     */
    @FunctionalInterface
    public interface CountFormulaStrategy {
        String calculate(MoneyFieldSumInfo info);
    }
} 