package io.github.luolong47.dbchecker.config;

import cn.hutool.core.util.StrUtil;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库查询WHERE条件配置
 */
@Data
@Slf4j
@ConfigurationProperties(prefix = "db")
public class DbConfig {

    private Map<String, Map<String, String>> where = new HashMap<>();

    /**
     * 包含配置
     */
    @NestedConfigurationProperty
    private Include include = new Include();

    /**
     * 导出配置
     */
    @NestedConfigurationProperty
    private Export export = new Export();

    /**
     * 运行模式配置
     */
    @NestedConfigurationProperty
    private Run run = new Run();

    /**
     * 重跑配置
     */
    @NestedConfigurationProperty
    private Rerun rerun = new Rerun();

    /**
     * 断点续跑配置
     */
    @NestedConfigurationProperty
    private Resume resume = new Resume();

    /**
     * 公式配置
     */
    @NestedConfigurationProperty
    private Formula formula = new Formula();

    /**
     * 初始化时记录已加载的条件
     */
    @PostConstruct
    public void init() {
        if (where == null || where.isEmpty()) {
            log.warn("未加载任何db.where条件配置");
        } else {
            log.info("已加载{}个数据源的条件配置:", where.size());
            for (Map.Entry<String, Map<String, String>> entry : where.entrySet()) {
                String dataSource = entry.getKey();
                Map<String, String> tableConditions = entry.getValue();
                log.info("数据源[{}]条件配置: {}", dataSource, tableConditions);
            }
        }

        // 记录其他配置信息
        log.debug("配置信息 - 包含Schema: {}", include.getSchemas());
        log.debug("配置信息 - 包含表: {}", include.getTables());
        log.debug("配置信息 - 导出目录: {}", export.getDirectory());
        log.debug("配置信息 - 运行模式: {}", run.getMode());
        log.debug("配置信息 - 重跑数据库: {}", rerun.getDatabases());
        log.debug("配置信息 - 断点续跑文件: {}", resume.getFile());
        log.debug("配置信息 - 公式1适用表: {}", formula.getFormula1());
        log.debug("配置信息 - 公式2适用表: {}", formula.getFormula2());
        log.debug("配置信息 - 公式3适用表: {}", formula.getFormula3());
        log.debug("配置信息 - 公式4适用表: {}", formula.getFormula4());
        log.debug("配置信息 - 公式5适用表: {}", formula.getFormula5());
        log.debug("配置信息 - 公式6适用表: {}", formula.getFormula6());
    }

    /**
     * 获取包含的schema列表
     */
    public String getIncludeSchemas() {
        return include.getSchemas();
    }

    /**
     * 获取包含的表名列表
     */
    public String getIncludeTables() {
        return include.getTables();
    }

    /**
     * 获取导出目录
     */
    public String getExportDirectory() {
        return export.getDirectory();
    }

    /**
     * 获取运行模式
     */
    public String getRunMode() {
        return run.getMode();
    }

    /**
     * 获取重跑数据库列表
     */
    public String getRerunDatabases() {
        return rerun.getDatabases();
    }

    /**
     * 获取断点续跑文件路径
     */
    public String getResumeFile() {
        return resume.getFile();
    }

    /**
     * 获取公式1适用表
     */
    public String getFormula1() {
        return formula.getFormula1();
    }

    /**
     * 获取公式2适用表
     */
    public String getFormula2() {
        return formula.getFormula2();
    }

    /**
     * 获取公式3适用表
     */
    public String getFormula3() {
        return formula.getFormula3();
    }

    /**
     * 获取公式4适用表
     */
    public String getFormula4() {
        return formula.getFormula4();
    }

    /**
     * 获取公式5适用表
     */
    public String getFormula5() {
        return formula.getFormula5();
    }

    /**
     * 获取公式6适用表
     */
    public String getFormula6() {
        return formula.getFormula6();
    }

    /**
     * 获取指定数据源和表的WHERE条件
     *
     * @param dataSourceName 数据源名称
     * @param tableName      表名
     * @return WHERE条件（不含WHERE关键字）或null
     */
    public String getCondition(String dataSourceName, String tableName) {
        // 忽略大小写查找数据源
        Map<String, String> tableConditions = null;
        for (Map.Entry<String, Map<String, String>> entry : where.entrySet()) {
            if (StrUtil.equalsIgnoreCase(entry.getKey(), dataSourceName)) {
                tableConditions = entry.getValue();
                log.debug("找到数据源 [{}] 的条件配置", dataSourceName);
                break;
            }
        }

        if (tableConditions != null) {
            // 忽略大小写查找表名
            for (Map.Entry<String, String> entry : tableConditions.entrySet()) {
                if (StrUtil.equalsIgnoreCase(entry.getKey(), tableName)) {
                    log.debug("找到表 [{}] 的条件: {}", tableName, entry.getValue());
                    return entry.getValue();
                }
            }
            log.debug("未找到表 [{}] 的条件配置", tableName);
        } else {
            log.debug("未找到数据源 [{}] 的条件配置", dataSourceName);
        }
        return null;
    }

    /**
     * 应用WHERE条件到SQL语句
     *
     * @param sql            原始SQL
     * @param dataSourceName 数据源名称
     * @param tableName      表名
     * @return 添加了WHERE条件的SQL
     */
    public String applyCondition(String sql, String dataSourceName, String tableName) {
        String condition = getCondition(dataSourceName, tableName);
        if (StrUtil.isNotEmpty(condition)) {
            String newSql;
            if (StrUtil.containsIgnoreCase(sql, " where ")) {
                newSql = sql + " AND " + condition;
            } else {
                newSql = sql + " WHERE " + condition;
            }
            log.info("应用WHERE条件 - 原SQL: [{}], 新SQL: [{}]", sql, newSql);
            return newSql;
        }
        return sql;
    }

    /**
     * 包含配置类
     */
    @Data
    public static class Include {
        /**
         * 要包含的schema列表，使用逗号分隔
         */
        private String schemas;

        /**
         * 要包含的表名列表，使用逗号分隔
         */
        private String tables;
    }

    /**
     * 导出配置类
     */
    @Data
    public static class Export {
        /**
         * 输出文件的目录，默认为当前目录
         */
        private String directory = ".";
    }

    /**
     * 运行模式配置类
     */
    @Data
    public static class Run {
        /**
         * 运行模式：RESUME(断点续跑), FULL(全量重跑), DB_NAME(指定库重跑)
         */
        private String mode = "RESUME";
    }

    /**
     * 重跑配置类
     */
    @Data
    public static class Rerun {
        /**
         * 指定要重跑的数据库，多个用逗号分隔
         */
        private String databases;
    }

    /**
     * 断点续跑配置类
     */
    @Data
    public static class Resume {
        /**
         * 断点续跑状态文件路径
         */
        private String file;
    }

    /**
     * 公式配置类
     */
    @Data
    public static class Formula {
        /**
         * 公式1适用表，使用逗号分隔
         */
        private String formula1;

        /**
         * 公式2适用表，使用逗号分隔
         */
        private String formula2;

        /**
         * 公式3适用表，使用逗号分隔
         */
        private String formula3;

        /**
         * 公式4适用表，使用逗号分隔
         */
        private String formula4;

        /**
         * 公式5适用表，使用逗号分隔
         */
        private String formula5;

        /**
         * 公式6适用表，使用逗号分隔
         */
        private String formula6;
    }
} 