package io.github.luolong47.dbchecker.service;

import io.github.luolong47.dbchecker.config.Dbconfig;
import io.github.luolong47.dbchecker.manager.DynamicDataSourceManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * 数据库初始化服务
 * 负责根据配置执行SQL脚本初始化数据库
 */
@Slf4j
@Service
@Order(0) // 使用最高优先级确保在其他组件之前初始化
@RequiredArgsConstructor
public class DatabaseInitService {

    private final Dbconfig dbconfig;
    private final DynamicDataSourceManager dataSourceManager;

    /**
     * 应用启动时初始化数据库
     */
    @PostConstruct
    public void init() {
        log.info("数据库初始化服务开始执行...");
        if (dbconfig.getInit() != null && dbconfig.getInit().isEnable()) {
            log.info("开始执行数据库初始化...");
            try {
                executeScripts();
                log.info("数据库初始化完成");
            } catch (Exception e) {
                log.error("数据库初始化过程中发生错误: {}", e.getMessage(), e);
            }
        } else {
            log.info("数据库初始化已禁用，跳过初始化步骤");
        }
    }

    /**
     * 执行所有配置的SQL脚本
     */
    private void executeScripts() {
        Map<String, List<String>> scripts = dbconfig.getInit().getScripts();
        if (scripts == null || scripts.isEmpty()) {
            log.warn("未配置初始化脚本，跳过初始化");
            return;
        }

        scripts.forEach(this::executeScriptsForDataSource);
    }

    /**
     * 执行指定数据源的SQL脚本
     * 
     * @param dataSourceName 数据源名称
     * @param scriptPaths 脚本路径列表
     */
    private void executeScriptsForDataSource(String dataSourceName, List<String> scriptPaths) {
        if (scriptPaths == null || scriptPaths.isEmpty()) {
            log.info("数据源[{}]没有配置初始化脚本，跳过", dataSourceName);
            return;
        }

        DataSource dataSource;
        try {
            dataSource = dataSourceManager.getDataSource(dataSourceName);
        } catch (IllegalArgumentException e) {
            log.error("初始化数据源[{}]失败: {}", dataSourceName, e.getMessage());
            return;
        }

        log.info("开始为数据源[{}]执行初始化脚本，共{}个脚本", dataSourceName, scriptPaths.size());
        for (String scriptPath : scriptPaths) {
            if (!StringUtils.hasText(scriptPath)) {
                continue;
            }
            
            try {
                Resource resource = new ClassPathResource(scriptPath);
                if (!resource.exists()) {
                    log.warn("脚本文件[{}]不存在，跳过执行", scriptPath);
                    continue;
                }

                log.info("执行脚本: {}", scriptPath);
                ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
                populator.addScript(resource);
                populator.setContinueOnError(true); // 允许脚本中的某些语句执行错误继续执行
                populator.setSeparator(";"); // 设置SQL语句分隔符
                populator.execute(dataSource);
                log.info("脚本[{}]执行完成", scriptPath);
            } catch (Exception e) {
                log.error("执行脚本[{}]失败: {}", scriptPath, e.getMessage(), e);
            }
        }
        log.info("数据源[{}]的初始化脚本执行完成", dataSourceName);
    }
}
