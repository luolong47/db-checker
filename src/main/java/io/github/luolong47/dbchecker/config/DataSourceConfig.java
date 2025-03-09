package io.github.luolong47.dbchecker.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据源配置类
 * 优雅地配置多个数据源及其对应的JdbcTemplate
 */
@Configuration
public class DataSourceConfig {

    /**
     * ora数据源配置
     */
    @Bean(name = "oraDataSource")
    @Primary
    @ConfigurationProperties(prefix = "spring.datasource.ora")
    public DataSource oraDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms-base数据源配置
     */
    @Bean(name = "rlcmsBaseDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-base")
    public DataSource rlcmsBaseDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms-pv1数据源配置
     */
    @Bean(name = "rlcmsPv1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv1")
    public DataSource rlcmsPv1DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms-pv2数据源配置
     */
    @Bean(name = "rlcmsPv2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv2")
    public DataSource rlcmsPv2DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms-pv3数据源配置
     */
    @Bean(name = "rlcmsPv3DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv3")
    public DataSource rlcmsPv3DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * bscopy-pv1数据源配置
     */
    @Bean(name = "bscopyPv1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv1")
    public DataSource bscopyPv1DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * bscopy-pv2数据源配置
     */
    @Bean(name = "bscopyPv2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv2")
    public DataSource bscopyPv2DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * bscopy-pv3数据源配置
     */
    @Bean(name = "bscopyPv3DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv3")
    public DataSource bscopyPv3DataSource() {
        return DataSourceBuilder.create().build();
    }
    
    /**
     * 所有数据源映射表 - 使用配置文件中的名称作为键
     */
    @Bean(name = "dataSourceMap")
    public Map<String, DataSource> dataSourceMap(
            @Qualifier("oraDataSource") DataSource oraDataSource,
            @Qualifier("rlcmsBaseDataSource") DataSource rlcmsBaseDataSource,
            @Qualifier("rlcmsPv1DataSource") DataSource rlcmsPv1DataSource,
            @Qualifier("rlcmsPv2DataSource") DataSource rlcmsPv2DataSource,
            @Qualifier("rlcmsPv3DataSource") DataSource rlcmsPv3DataSource,
            @Qualifier("bscopyPv1DataSource") DataSource bscopyPv1DataSource,
            @Qualifier("bscopyPv2DataSource") DataSource bscopyPv2DataSource,
            @Qualifier("bscopyPv3DataSource") DataSource bscopyPv3DataSource) {
        
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        dataSourceMap.put("ora", oraDataSource);
        dataSourceMap.put("rlcms-base", rlcmsBaseDataSource);
        dataSourceMap.put("rlcms-pv1", rlcmsPv1DataSource);
        dataSourceMap.put("rlcms-pv2", rlcmsPv2DataSource);
        dataSourceMap.put("rlcms-pv3", rlcmsPv3DataSource);
        dataSourceMap.put("bscopy-pv1", bscopyPv1DataSource);
        dataSourceMap.put("bscopy-pv2", bscopyPv2DataSource);
        dataSourceMap.put("bscopy-pv3", bscopyPv3DataSource);
        
        return dataSourceMap;
    }

    /**
     * 配置ora JdbcTemplate
     */
    @Bean(name = "oraJdbcTemplate")
    @Primary
    public JdbcTemplate oraJdbcTemplate(@Qualifier("oraDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置rlcms-base JdbcTemplate
     */
    @Bean(name = "rlcmsBaseJdbcTemplate")
    public JdbcTemplate rlcmsBaseJdbcTemplate(@Qualifier("rlcmsBaseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置rlcms-pv1 JdbcTemplate
     */
    @Bean(name = "rlcmsPv1JdbcTemplate")
    public JdbcTemplate rlcmsPv1JdbcTemplate(@Qualifier("rlcmsPv1DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置rlcms-pv2 JdbcTemplate
     */
    @Bean(name = "rlcmsPv2JdbcTemplate")
    public JdbcTemplate rlcmsPv2JdbcTemplate(@Qualifier("rlcmsPv2DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置rlcms-pv3 JdbcTemplate
     */
    @Bean(name = "rlcmsPv3JdbcTemplate")
    public JdbcTemplate rlcmsPv3JdbcTemplate(@Qualifier("rlcmsPv3DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置bscopy-pv1 JdbcTemplate
     */
    @Bean(name = "bscopyPv1JdbcTemplate")
    public JdbcTemplate bscopyPv1JdbcTemplate(@Qualifier("bscopyPv1DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置bscopy-pv2 JdbcTemplate
     */
    @Bean(name = "bscopyPv2JdbcTemplate")
    public JdbcTemplate bscopyPv2JdbcTemplate(@Qualifier("bscopyPv2DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 配置bscopy-pv3 JdbcTemplate
     */
    @Bean(name = "bscopyPv3JdbcTemplate")
    public JdbcTemplate bscopyPv3JdbcTemplate(@Qualifier("bscopyPv3DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
