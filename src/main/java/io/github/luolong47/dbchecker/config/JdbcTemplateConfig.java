package io.github.luolong47.dbchecker.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * JdbcTemplate配置类
 */
@Configuration
public class JdbcTemplateConfig {

    /**
     * 创建ora数据源的JdbcTemplate
     */
    @Primary
    @Bean("oraJdbcTemplate")
    public JdbcTemplate oraJdbcTemplate(@Qualifier("oraDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建ora-slave数据源的JdbcTemplate
     */
    @Bean("oraSlaveJdbcTemplate")
    public JdbcTemplate oraSlaveJdbcTemplate(@Qualifier("oraSlaveDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建rlcms-base数据源的JdbcTemplate
     */
    @Bean("rlcmsBaseJdbcTemplate")
    public JdbcTemplate rlcmsBaseJdbcTemplate(@Qualifier("rlcmsBaseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建rlcms-pv1数据源的JdbcTemplate
     */
    @Bean("rlcmsPv1JdbcTemplate")
    public JdbcTemplate rlcmsPv1JdbcTemplate(@Qualifier("rlcmsPv1DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建rlcms-pv2数据源的JdbcTemplate
     */
    @Bean("rlcmsPv2JdbcTemplate")
    public JdbcTemplate rlcmsPv2JdbcTemplate(@Qualifier("rlcmsPv2DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建rlcms-pv3数据源的JdbcTemplate
     */
    @Bean("rlcmsPv3JdbcTemplate")
    public JdbcTemplate rlcmsPv3JdbcTemplate(@Qualifier("rlcmsPv3DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建bscopy-pv1数据源的JdbcTemplate
     */
    @Bean("bscopyPv1JdbcTemplate")
    public JdbcTemplate bscopyPv1JdbcTemplate(@Qualifier("bscopyPv1DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建bscopy-pv2数据源的JdbcTemplate
     */
    @Bean("bscopyPv2JdbcTemplate")
    public JdbcTemplate bscopyPv2JdbcTemplate(@Qualifier("bscopyPv2DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
    
    /**
     * 创建bscopy-pv3数据源的JdbcTemplate
     */
    @Bean("bscopyPv3JdbcTemplate")
    public JdbcTemplate bscopyPv3JdbcTemplate(@Qualifier("bscopyPv3DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
} 