package io.github.luolong47.dbchecker.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfig {

    @Bean(name = "oraHikariConfig")
    @ConfigurationProperties("spring.datasource.sources.ora")
    public HikariConfig oraHikariConfig() {
        return new HikariConfig();
    }

    @Primary
    @Bean(name = "oraDataSource")
    public DataSource oraDataSource(@Qualifier("oraHikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // ora-slave 数据源配置
    @Bean(name = "oraSlaveHikariConfig")
    @ConfigurationProperties("spring.datasource.sources.ora-slave")
    public HikariConfig oraSlaveHikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "oraSlaveDataSource")
    public DataSource oraSlaveDataSource(@Qualifier("oraSlaveHikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // rlcms-base 数据源配置
    @Bean(name = "rlcmsBaseHikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-base")
    public HikariConfig rlcmsBaseHikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsBaseDataSource")
    public DataSource rlcmsBaseDataSource(@Qualifier("rlcmsBaseHikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // rlcms-pv1 数据源配置
    @Bean(name = "rlcmsPv1HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-pv1")
    public HikariConfig rlcmsPv1HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsPv1DataSource")
    public DataSource rlcmsPv1DataSource(@Qualifier("rlcmsPv1HikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // rlcms-pv2 数据源配置
    @Bean(name = "rlcmsPv2HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-pv2")
    public HikariConfig rlcmsPv2HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsPv2DataSource")
    public DataSource rlcmsPv2DataSource(@Qualifier("rlcmsPv2HikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // rlcms-pv3 数据源配置
    @Bean(name = "rlcmsPv3HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-pv3")
    public HikariConfig rlcmsPv3HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsPv3DataSource")
    public DataSource rlcmsPv3DataSource(@Qualifier("rlcmsPv3HikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // bscopy-pv1 数据源配置
    @Bean(name = "bscopyPv1HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.bscopy-pv1")
    public HikariConfig bscopyPv1HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "bscopyPv1DataSource")
    public DataSource bscopyPv1DataSource(@Qualifier("bscopyPv1HikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // bscopy-pv2 数据源配置
    @Bean(name = "bscopyPv2HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.bscopy-pv2")
    public HikariConfig bscopyPv2HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "bscopyPv2DataSource")
    public DataSource bscopyPv2DataSource(@Qualifier("bscopyPv2HikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }

    // bscopy-pv3 数据源配置
    @Bean(name = "bscopyPv3HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.bscopy-pv3")
    public HikariConfig bscopyPv3HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "bscopyPv3DataSource")
    public DataSource bscopyPv3DataSource(@Qualifier("bscopyPv3HikariConfig") HikariConfig config) {
        return new HikariDataSource(config);
    }
}
