package io.github.luolong47.dbchecker.config;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.datasource.init.ScriptUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 数据库初始化器
 * 用于在应用启动时执行各数据源的初始化SQL脚本
 */
@Component
@Order(1)  // 确保在应用启动早期执行
@Slf4j
public class DatabaseInitializer implements CommandLineRunner {

    @javax.annotation.Resource
    private DatabaseInitScriptsProperties initProperties;

    @Qualifier("dataSourceMap")
    @Autowired
    private Map<String, DataSource> dataSourceMap;

    @Override
    public void run(String... args) throws Exception {
        if (!initProperties.isEnabled()) {
            log.info("数据库初始化已禁用");
            return;
        }

        Map<String, List<String>> scripts = initProperties.getScripts();
        if (CollectionUtils.isEmpty(scripts)) {
            log.warn("未配置任何初始化脚本");
            return;
        }
        
        // 记录所有可用的数据源键名，帮助排查问题
        log.info("可用的数据源键名: {}", 
                 dataSourceMap.keySet().stream().sorted().collect(Collectors.toList()));
        log.info("配置的脚本数据源键名: {}", 
                 scripts.keySet().stream().sorted().collect(Collectors.toList()));

        StopWatch stopWatch = new StopWatch("数据库初始化");
        
        // 执行各数据源的初始化脚本
        for (Map.Entry<String, List<String>> entry : scripts.entrySet()) {
            String dataSourceName = entry.getKey();
            List<String> scriptPaths = entry.getValue();

            if (CollectionUtils.isEmpty(scriptPaths)) {
                continue;
            }

            DataSource dataSource = dataSourceMap.get(dataSourceName);
            if (dataSource == null) {
                log.warn("数据源 '{}' 不存在，跳过初始化脚本。可用的数据源: {}", 
                         dataSourceName, StrUtil.join(", ", dataSourceMap.keySet()));
                continue;
            }

            stopWatch.start(dataSourceName);
            try {
                executeScripts(dataSource, scriptPaths, dataSourceName);
            } finally {
                stopWatch.stop();
            }
        }

        log.info("数据库初始化完成: \n{}", stopWatch.prettyPrint());
    }

    /**
     * 执行指定数据源的SQL脚本
     */
    private void executeScripts(DataSource dataSource, List<String> scriptPaths, String dataSourceName) {
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            populator.setContinueOnError(true);
            populator.setSeparator(ScriptUtils.DEFAULT_STATEMENT_SEPARATOR);
            populator.setCommentPrefix(ScriptUtils.DEFAULT_COMMENT_PREFIX);
            populator.setBlockCommentStartDelimiter(ScriptUtils.DEFAULT_BLOCK_COMMENT_START_DELIMITER);
            populator.setBlockCommentEndDelimiter(ScriptUtils.DEFAULT_BLOCK_COMMENT_END_DELIMITER);

            int validScripts = 0;
            for (String path : scriptPaths) {
                Resource resource = new ClassPathResource(path);
                if (!resource.exists()) {
                    log.warn("脚本文件 '{}' 不存在，跳过", path);
                    continue;
                }
                
                try {
                    log.info("添加脚本: {} 到数据源: {} (大小: {} 字节)", 
                             path, dataSourceName, resource.contentLength());
                    populator.addScript(resource);
                    validScripts++;
                } catch (IOException e) {
                    log.warn("读取脚本文件 '{}' 失败: {}", path, e.getMessage());
                }
            }

            if (validScripts == 0) {
                log.warn("数据源 '{}' 没有有效的脚本文件", dataSourceName);
                return;
            }

            log.info("开始执行数据源 '{}' 的 {} 个初始化脚本", dataSourceName, validScripts);
            populator.execute(dataSource);
            log.info("数据源 '{}' 的初始化脚本执行完成", dataSourceName);

        } catch (Exception e) {
            log.error("执行数据源 '{}' 的初始化脚本时发生错误: {}", dataSourceName, e.getMessage(), e);
        }
    }
} 