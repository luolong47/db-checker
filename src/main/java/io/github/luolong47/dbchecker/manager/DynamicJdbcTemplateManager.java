package io.github.luolong47.dbchecker.manager;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态JdbcTemplate管理器
 * 根据数据源名称获取对应的JdbcTemplate
 */
@Slf4j
@Component
public class DynamicJdbcTemplateManager {

    private final Map<String, JdbcTemplate> jdbcTemplates = new ConcurrentHashMap<>();
    private final DynamicDataSourceManager dataSourceManager;

    @Autowired
    public DynamicJdbcTemplateManager(
            DynamicDataSourceManager dataSourceManager,
            @Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate,
            @Qualifier("oraSlaveJdbcTemplate") JdbcTemplate oraSlaveJdbcTemplate,
            @Qualifier("rlcmsBaseJdbcTemplate") JdbcTemplate rlcmsBaseJdbcTemplate,
            @Qualifier("rlcmsPv1JdbcTemplate") JdbcTemplate rlcmsPv1JdbcTemplate,
            @Qualifier("rlcmsPv2JdbcTemplate") JdbcTemplate rlcmsPv2JdbcTemplate,
            @Qualifier("rlcmsPv3JdbcTemplate") JdbcTemplate rlcmsPv3JdbcTemplate,
            @Qualifier("bscopyPv1JdbcTemplate") JdbcTemplate bscopyPv1JdbcTemplate,
            @Qualifier("bscopyPv2JdbcTemplate") JdbcTemplate bscopyPv2JdbcTemplate,
            @Qualifier("bscopyPv3JdbcTemplate") JdbcTemplate bscopyPv3JdbcTemplate) {
        
        this.dataSourceManager = dataSourceManager;
        
        // 只注册已启用的数据源的JdbcTemplate
        registerJdbcTemplateIfDataSourceExists("ora", oraJdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("ora-slave", oraSlaveJdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("rlcms-base", rlcmsBaseJdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("rlcms-pv1", rlcmsPv1JdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("rlcms-pv2", rlcmsPv2JdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("rlcms-pv3", rlcmsPv3JdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("bscopy-pv1", bscopyPv1JdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("bscopy-pv2", bscopyPv2JdbcTemplate);
        registerJdbcTemplateIfDataSourceExists("bscopy-pv3", bscopyPv3JdbcTemplate);
    }
    
    /**
     * 只在数据源已启用时注册对应的JdbcTemplate
     * @param dataSourceName 数据源名称
     * @param jdbcTemplate JdbcTemplate实例
     */
    private void registerJdbcTemplateIfDataSourceExists(String dataSourceName, JdbcTemplate jdbcTemplate) {
        if (dataSourceManager.hasDataSource(dataSourceName)) {
            jdbcTemplates.put(dataSourceName, jdbcTemplate);
            log.info("已注册JdbcTemplate: [{}]", dataSourceName);
        } else {
            log.info("数据源 [{}] 未启用或不存在，跳过注册JdbcTemplate", dataSourceName);
        }
    }

    /**
     * 获取指定数据源的JdbcTemplate
     * @param dataSourceName 数据源名称
     * @return JdbcTemplate实例
     */
    public JdbcTemplate getJdbcTemplate(String dataSourceName) {
        JdbcTemplate jdbcTemplate = jdbcTemplates.get(dataSourceName);
        if (jdbcTemplate == null) {
            throw new IllegalArgumentException("数据源 [" + dataSourceName + "] 的JdbcTemplate不存在或数据源已禁用");
        }
        return jdbcTemplate;
    }

    /**
     * 获取主数据源的JdbcTemplate
     * @return 主JdbcTemplate实例
     */
    public JdbcTemplate getPrimaryJdbcTemplate() {
        return getJdbcTemplate("ora");
    }

    /**
     * 获取所有JdbcTemplate
     * @return JdbcTemplate Map
     */
    public Map<String, JdbcTemplate> getAllJdbcTemplates() {
        return new HashMap<>(jdbcTemplates);
    }
    
    /**
     * 检查JdbcTemplate是否存在
     * @param dataSourceName 数据源名称
     * @return 是否存在
     */
    public boolean hasJdbcTemplate(String dataSourceName) {
        return jdbcTemplates.containsKey(dataSourceName);
    }
} 
