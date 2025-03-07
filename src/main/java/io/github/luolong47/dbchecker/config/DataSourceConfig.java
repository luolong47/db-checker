package io.github.luolong47.dbchecker.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.List;

/**
 * 多数据源配置类
 */
@Configuration
public class DataSourceConfig {

    @Autowired
    private DatabaseInitScriptsProperties databaseInitScriptsProperties;
    
    /**
     * 全局Druid数据源配置
     */
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.druid")
    public DruidDataSource globalDruidProperties() {
        return DruidDataSourceBuilder.create().build();
    }

    /**
     * ora数据源（主数据源）
     */
    @Primary
    @Bean(name = "oraDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.ora")
    public DataSource oraDataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * rlcms_base数据源
     */
    @Bean(name = "rlcmsBaseDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-base")
    public DataSource rlcmsBaseDataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * rlcms_pv1数据源
     */
    @Bean(name = "rlcmsPv1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv1")
    public DataSource rlcmsPv1DataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * rlcms_pv2数据源
     */
    @Bean(name = "rlcmsPv2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv2")
    public DataSource rlcmsPv2DataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * rlcms_pv3数据源
     */
    @Bean(name = "rlcmsPv3DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.rlcms-pv3")
    public DataSource rlcmsPv3DataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * bscopy_pv1数据源
     */
    @Bean(name = "bscopyPv1DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv1")
    public DataSource bscopyPv1DataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * bscopy_pv2数据源
     */
    @Bean(name = "bscopyPv2DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv2")
    public DataSource bscopyPv2DataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }

    /**
     * bscopy_pv3数据源
     */
    @Bean(name = "bscopyPv3DataSource")
    @ConfigurationProperties(prefix = "spring.datasource.bscopy-pv3")
    public DataSource bscopyPv3DataSource() {
        // 创建数据源并应用全局配置
        DruidDataSource dataSource = DruidDataSourceBuilder.create().build();
        applyGlobalDruidConfig(dataSource);
        return dataSource;
    }
    
    /**
     * 将全局Druid配置应用到数据源
     * @param dataSource 数据源
     */
    private void applyGlobalDruidConfig(DruidDataSource dataSource) {
        DruidDataSource globalConfig = globalDruidProperties();
        if (globalConfig == null) {
            return;
        }
        
        // 如果数据源没有配置validationQuery，使用全局配置的
        if (dataSource.getValidationQuery() == null && globalConfig.getValidationQuery() != null) {
            dataSource.setValidationQuery(globalConfig.getValidationQuery());
        }
        
        // 如果数据源没有配置initialSize，使用全局配置的
        if (dataSource.getInitialSize() == 0 && globalConfig.getInitialSize() > 0) {
            dataSource.setInitialSize(globalConfig.getInitialSize());
        }
        
        // 如果数据源没有配置minIdle，使用全局配置的
        if (dataSource.getMinIdle() == 0 && globalConfig.getMinIdle() > 0) {
            dataSource.setMinIdle(globalConfig.getMinIdle());
        }
        
        // 如果数据源没有配置maxActive，使用全局配置的
        if (dataSource.getMaxActive() == 8 && globalConfig.getMaxActive() > 0) {
            dataSource.setMaxActive(globalConfig.getMaxActive());
        }
        
        // 如果数据源没有配置maxWait，使用全局配置的
        if (dataSource.getMaxWait() == -1 && globalConfig.getMaxWait() > 0) {
            dataSource.setMaxWait(globalConfig.getMaxWait());
        }
        
        // 如果数据源没有配置timeBetweenEvictionRunsMillis，使用全局配置的
        if (dataSource.getTimeBetweenEvictionRunsMillis() == 60000 && globalConfig.getTimeBetweenEvictionRunsMillis() > 0) {
            dataSource.setTimeBetweenEvictionRunsMillis(globalConfig.getTimeBetweenEvictionRunsMillis());
        }
        
        // 如果数据源没有配置minEvictableIdleTimeMillis，使用全局配置的
        if (dataSource.getMinEvictableIdleTimeMillis() == 1800000 && globalConfig.getMinEvictableIdleTimeMillis() > 0) {
            dataSource.setMinEvictableIdleTimeMillis(globalConfig.getMinEvictableIdleTimeMillis());
        }
        
        // 如果数据源没有配置maxEvictableIdleTimeMillis，使用全局配置的
        if (dataSource.getMaxEvictableIdleTimeMillis() == 25200000 && globalConfig.getMaxEvictableIdleTimeMillis() > 0) {
            dataSource.setMaxEvictableIdleTimeMillis(globalConfig.getMaxEvictableIdleTimeMillis());
        }
        
        // 如果数据源没有配置testWhileIdle，使用全局配置的
        if (!dataSource.isTestWhileIdle()) {
            dataSource.setTestWhileIdle(globalConfig.isTestWhileIdle());
        }
        
        // 如果数据源没有配置testOnBorrow，使用全局配置的
        if (!dataSource.isTestOnBorrow()) {
            dataSource.setTestOnBorrow(globalConfig.isTestOnBorrow());
        }
        
        // 如果数据源没有配置testOnReturn，使用全局配置的
        if (!dataSource.isTestOnReturn()) {
            dataSource.setTestOnReturn(globalConfig.isTestOnReturn());
        }
        
        // 如果数据源没有配置poolPreparedStatements，使用全局配置的
        if (!dataSource.isPoolPreparedStatements()) {
            dataSource.setPoolPreparedStatements(globalConfig.isPoolPreparedStatements());
        }
        
        // 如果数据源没有配置maxPoolPreparedStatementPerConnectionSize，使用全局配置的
        if (dataSource.getMaxPoolPreparedStatementPerConnectionSize() == -1 && globalConfig.getMaxPoolPreparedStatementPerConnectionSize() > 0) {
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(globalConfig.getMaxPoolPreparedStatementPerConnectionSize());
        }
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