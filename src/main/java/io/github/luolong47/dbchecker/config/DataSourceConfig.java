package io.github.luolong47.dbchecker.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.List;

/**
 * 多数据源配置类
 */
@Configuration
public class DataSourceConfig {

    @Autowired
    private Environment env;

    @Autowired
    private DatabaseInitScriptsProperties databaseInitScriptsProperties;

    /**
     * ora数据源（主数据源）
     */
    @Primary
    @Bean(name = "oraDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.ora")
    public DataSource oraDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms_base数据源
     */
    @Bean(name = "rlcmsBaseDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-base")
    public DataSource rlcmsBaseDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms_pv1数据源
     */
    @Bean(name = "rlcmsPv1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv1")
    public DataSource rlcmsPv1DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms_pv2数据源
     */
    @Bean(name = "rlcmsPv2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv2")
    public DataSource rlcmsPv2DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * rlcms_pv3数据源
     */
    @Bean(name = "rlcmsPv3DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv3")
    public DataSource rlcmsPv3DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * bscopy_pv1数据源
     */
    @Bean(name = "bscopyPv1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv1")
    public DataSource bscopyPv1DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * bscopy_pv2数据源
     */
    @Bean(name = "bscopyPv2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv2")
    public DataSource bscopyPv2DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * bscopy_pv3数据源
     */
    @Bean(name = "bscopyPv3DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv3")
    public DataSource bscopyPv3DataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * ora数据源初始化器
     */
    @Bean
    public DataSourceInitializer oraDataSourceInitializer(@Qualifier("oraDataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "ora");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * rlcms_base数据源初始化器
     */
    @Bean
    public DataSourceInitializer rlcmsBaseDataSourceInitializer(@Qualifier("rlcmsBaseDataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "rlcms-base");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * rlcms_pv1数据源初始化器
     */
    @Bean
    public DataSourceInitializer rlcmsPv1DataSourceInitializer(@Qualifier("rlcmsPv1DataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "rlcms-pv1");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * rlcms_pv2数据源初始化器
     */
    @Bean
    public DataSourceInitializer rlcmsPv2DataSourceInitializer(@Qualifier("rlcmsPv2DataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "rlcms-pv2");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * rlcms_pv3数据源初始化器
     */
    @Bean
    public DataSourceInitializer rlcmsPv3DataSourceInitializer(@Qualifier("rlcmsPv3DataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "rlcms-pv3");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * bscopy_pv1数据源初始化器
     */
    @Bean
    public DataSourceInitializer bscopyPv1DataSourceInitializer(@Qualifier("bscopyPv1DataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "bscopy-pv1");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * bscopy_pv2数据源初始化器
     */
    @Bean
    public DataSourceInitializer bscopyPv2DataSourceInitializer(@Qualifier("bscopyPv2DataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "bscopy-pv2");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * bscopy_pv3数据源初始化器
     */
    @Bean
    public DataSourceInitializer bscopyPv3DataSourceInitializer(@Qualifier("bscopyPv3DataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        addScriptsToPopulator(populator, "bscopy-pv3");
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        
        // 从配置类获取是否启用初始化
        initializer.setEnabled(databaseInitScriptsProperties.isEnabled());
        
        return initializer;
    }

    /**
     * 将指定数据源的脚本添加到populator
     * @param populator ResourceDatabasePopulator
     * @param datasourceName 数据源名称
     */
    private void addScriptsToPopulator(ResourceDatabasePopulator populator, String datasourceName) {
        List<String> scripts = databaseInitScriptsProperties.getScriptsForDatasource(datasourceName);
        if (scripts != null) {
            for (String scriptPath : scripts) {
                if (StringUtils.hasText(scriptPath)) {
                    populator.addScript(new ClassPathResource(scriptPath));
                }
            }
        }
    }

    /**
     * ora数据源JdbcTemplate（主）
     */
    @Primary
    @Bean(name = "oraJdbcTemplate")
    public JdbcTemplate oraJdbcTemplate(@Qualifier("oraDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * rlcms_base数据源JdbcTemplate
     */
    @Bean(name = "rlcmsBaseJdbcTemplate")
    public JdbcTemplate rlcmsBaseJdbcTemplate(@Qualifier("rlcmsBaseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * rlcms_pv1数据源JdbcTemplate
     */
    @Bean(name = "rlcmsPv1JdbcTemplate")
    public JdbcTemplate rlcmsPv1JdbcTemplate(@Qualifier("rlcmsPv1DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * rlcms_pv2数据源JdbcTemplate
     */
    @Bean(name = "rlcmsPv2JdbcTemplate")
    public JdbcTemplate rlcmsPv2JdbcTemplate(@Qualifier("rlcmsPv2DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * rlcms_pv3数据源JdbcTemplate
     */
    @Bean(name = "rlcmsPv3JdbcTemplate")
    public JdbcTemplate rlcmsPv3JdbcTemplate(@Qualifier("rlcmsPv3DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * bscopy_pv1数据源JdbcTemplate
     */
    @Bean(name = "bscopyPv1JdbcTemplate")
    public JdbcTemplate bscopyPv1JdbcTemplate(@Qualifier("bscopyPv1DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * bscopy_pv2数据源JdbcTemplate
     */
    @Bean(name = "bscopyPv2JdbcTemplate")
    public JdbcTemplate bscopyPv2JdbcTemplate(@Qualifier("bscopyPv2DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * bscopy_pv3数据源JdbcTemplate
     */
    @Bean(name = "bscopyPv3JdbcTemplate")
    public JdbcTemplate bscopyPv3JdbcTemplate(@Qualifier("bscopyPv3DataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
} 