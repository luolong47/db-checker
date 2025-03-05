package io.github.luolong47.dbchecker.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;

/**
 * 多数据源配置类
 */
@Configuration
public class DataSourceConfig {

    /**
     * 主数据源
     */
    @Primary
    @Bean(name = "primaryDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.primary")
    public DataSource primaryDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * 第二个数据源
     */
    @Bean(name = "secondaryDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.secondary")
    public DataSource secondaryDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * 第三个数据源
     */
    @Bean(name = "tertiaryDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.tertiary")
    public DataSource tertiaryDataSource() {
        return DataSourceBuilder.create().build();
    }

    /**
     * 主数据源初始化器
     */
    @Bean
    public DataSourceInitializer primaryDataSourceInitializer(@Qualifier("primaryDataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("db/schema-primary.sql"));
        populator.addScript(new ClassPathResource("db/data-primary.sql"));
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        initializer.setEnabled(true);
        
        return initializer;
    }

    /**
     * 第二个数据源初始化器
     */
    @Bean
    public DataSourceInitializer secondaryDataSourceInitializer(@Qualifier("secondaryDataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("db/schema-secondary.sql"));
        populator.addScript(new ClassPathResource("db/data-secondary.sql"));
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        initializer.setEnabled(true);
        
        return initializer;
    }

    /**
     * 第三个数据源初始化器
     */
    @Bean
    public DataSourceInitializer tertiaryDataSourceInitializer(@Qualifier("tertiaryDataSource") DataSource dataSource) {
        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("db/schema-tertiary.sql"));
        populator.addScript(new ClassPathResource("db/data-tertiary.sql"));
        populator.setContinueOnError(false);
        populator.setSeparator(";");
        initializer.setDatabasePopulator(populator);
        initializer.setEnabled(true);
        
        return initializer;
    }

    /**
     * 主数据源JdbcTemplate
     */
    @Primary
    @Bean(name = "primaryJdbcTemplate")
    public JdbcTemplate primaryJdbcTemplate(@Qualifier("primaryDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 第二个数据源JdbcTemplate
     */
    @Bean(name = "secondaryJdbcTemplate")
    public JdbcTemplate secondaryJdbcTemplate(@Qualifier("secondaryDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    /**
     * 第三个数据源JdbcTemplate
     */
    @Bean(name = "tertiaryJdbcTemplate")
    public JdbcTemplate tertiaryJdbcTemplate(@Qualifier("tertiaryDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
} 