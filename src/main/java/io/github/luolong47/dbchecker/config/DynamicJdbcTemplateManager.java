package io.github.luolong47.dbchecker.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 动态JdbcTemplate管理器
 * 根据数据源名称获取对应的JdbcTemplate
 */
@Component
public class DynamicJdbcTemplateManager {

    private final Map<String, JdbcTemplate> jdbcTemplates = new HashMap<>();

    @Autowired
    public DynamicJdbcTemplateManager(
            @Qualifier("oraJdbcTemplate") JdbcTemplate oraJdbcTemplate,
            @Qualifier("oraSlaveJdbcTemplate") JdbcTemplate oraSlaveJdbcTemplate,
            @Qualifier("rlcmsBaseJdbcTemplate") JdbcTemplate rlcmsBaseJdbcTemplate,
            @Qualifier("rlcmsPv1JdbcTemplate") JdbcTemplate rlcmsPv1JdbcTemplate,
            @Qualifier("rlcmsPv2JdbcTemplate") JdbcTemplate rlcmsPv2JdbcTemplate,
            @Qualifier("rlcmsPv3JdbcTemplate") JdbcTemplate rlcmsPv3JdbcTemplate,
            @Qualifier("bscopyPv1JdbcTemplate") JdbcTemplate bscopyPv1JdbcTemplate,
            @Qualifier("bscopyPv2JdbcTemplate") JdbcTemplate bscopyPv2JdbcTemplate,
            @Qualifier("bscopyPv3JdbcTemplate") JdbcTemplate bscopyPv3JdbcTemplate) {
        
        // 注册所有JdbcTemplate
        jdbcTemplates.put("ora", oraJdbcTemplate);
        jdbcTemplates.put("ora-slave", oraSlaveJdbcTemplate);
        jdbcTemplates.put("rlcms-base", rlcmsBaseJdbcTemplate);
        jdbcTemplates.put("rlcms-pv1", rlcmsPv1JdbcTemplate);
        jdbcTemplates.put("rlcms-pv2", rlcmsPv2JdbcTemplate);
        jdbcTemplates.put("rlcms-pv3", rlcmsPv3JdbcTemplate);
        jdbcTemplates.put("bscopy-pv1", bscopyPv1JdbcTemplate);
        jdbcTemplates.put("bscopy-pv2", bscopyPv2JdbcTemplate);
        jdbcTemplates.put("bscopy-pv3", bscopyPv3JdbcTemplate);
    }

    /**
     * 获取指定数据源的JdbcTemplate
     * @param dataSourceName 数据源名称
     * @return JdbcTemplate实例
     */
    public JdbcTemplate getJdbcTemplate(String dataSourceName) {
        JdbcTemplate jdbcTemplate = jdbcTemplates.get(dataSourceName);
        if (jdbcTemplate == null) {
            throw new IllegalArgumentException("数据源 [" + dataSourceName + "] 的JdbcTemplate不存在");
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
} 