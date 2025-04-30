package io.github.luolong47.dbchecker.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

@Slf4j
@Configuration
public class DataSourceConfig {

    private final Environment env;

    public DataSourceConfig(Environment env) {
        this.env = env;
    }

    @Bean(name = "oraHikariConfig")
    @ConfigurationProperties("spring.datasource.sources.ora")
    public HikariConfig oraHikariConfig() {
        return new HikariConfig();
    }

    @Primary
    @Bean(name = "oraDataSource")
    public DataSource oraDataSource(@Qualifier("oraHikariConfig") HikariConfig config) {
        return createDataSource(config, "ora");
    }

    // ora-slave 数据源配置
    @Bean(name = "oraSlaveHikariConfig")
    @ConfigurationProperties("spring.datasource.sources.ora-slave")
    public HikariConfig oraSlaveHikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "oraSlaveDataSource")
    public DataSource oraSlaveDataSource(@Qualifier("oraSlaveHikariConfig") HikariConfig config) {
        return createDataSource(config, "ora-slave");
    }

    // rlcms-base 数据源配置
    @Bean(name = "rlcmsBaseHikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-base")
    public HikariConfig rlcmsBaseHikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsBaseDataSource")
    public DataSource rlcmsBaseDataSource(@Qualifier("rlcmsBaseHikariConfig") HikariConfig config) {
        return createDataSource(config, "rlcms-base");
    }

    // rlcms-pv1 数据源配置
    @Bean(name = "rlcmsPv1HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-pv1")
    public HikariConfig rlcmsPv1HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsPv1DataSource")
    public DataSource rlcmsPv1DataSource(@Qualifier("rlcmsPv1HikariConfig") HikariConfig config) {
        return createDataSource(config, "rlcms-pv1");
    }

    // rlcms-pv2 数据源配置
    @Bean(name = "rlcmsPv2HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-pv2")
    public HikariConfig rlcmsPv2HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsPv2DataSource")
    public DataSource rlcmsPv2DataSource(@Qualifier("rlcmsPv2HikariConfig") HikariConfig config) {
        return createDataSource(config, "rlcms-pv2");
    }

    // rlcms-pv3 数据源配置
    @Bean(name = "rlcmsPv3HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.rlcms-pv3")
    public HikariConfig rlcmsPv3HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "rlcmsPv3DataSource")
    public DataSource rlcmsPv3DataSource(@Qualifier("rlcmsPv3HikariConfig") HikariConfig config) {
        return createDataSource(config, "rlcms-pv3");
    }

    // bscopy-pv1 数据源配置
    @Bean(name = "bscopyPv1HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.bscopy-pv1")
    public HikariConfig bscopyPv1HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "bscopyPv1DataSource")
    public DataSource bscopyPv1DataSource(@Qualifier("bscopyPv1HikariConfig") HikariConfig config) {
        return createDataSource(config, "bscopy-pv1");
    }

    // bscopy-pv2 数据源配置
    @Bean(name = "bscopyPv2HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.bscopy-pv2")
    public HikariConfig bscopyPv2HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "bscopyPv2DataSource")
    public DataSource bscopyPv2DataSource(@Qualifier("bscopyPv2HikariConfig") HikariConfig config) {
        return createDataSource(config, "bscopy-pv2");
    }

    // bscopy-pv3 数据源配置
    @Bean(name = "bscopyPv3HikariConfig")
    @ConfigurationProperties("spring.datasource.sources.bscopy-pv3")
    public HikariConfig bscopyPv3HikariConfig() {
        return new HikariConfig();
    }

    @Bean(name = "bscopyPv3DataSource")
    public DataSource bscopyPv3DataSource(@Qualifier("bscopyPv3HikariConfig") HikariConfig config) {
        return createDataSource(config, "bscopy-pv3");
    }
    
    /**
     * 根据配置创建数据源，并处理enable属性
     * 
     * @param config Hikari配置
     * @param name 数据源名称
     * @return 数据源实例
     */
    private DataSource createDataSource(HikariConfig config, String name) {
        // 直接从环境变量中读取enable配置，优先于HikariConfig中的属性
        String configPath = "spring.datasource.sources." + name + ".enable";
        boolean isEnabled = env.getProperty(configPath, Boolean.class, true); // 默认为true
        
        log.info("数据源 [{}] enable状态: {}", name, isEnabled);
        
        // 如果数据源已禁用，返回禁用数据源实例
        if (!isEnabled) {
            log.warn("数据源 [{}] 已禁用，创建虚拟数据源", name);
            return new DisabledDataSource(name);
        }
        
        // 数据源已启用，创建并返回真实数据源
        log.info("创建数据源 [{}]", name);
        return new HikariDataSource(config);
    }
    
    /**
     * 已禁用的数据源的实现类
     * 所有方法都会抛出异常，表示该数据源已被禁用
     */
    private static class DisabledDataSource implements DataSource {
        private final String name;
        
        public DisabledDataSource(String name) {
            this.name = name;
        }
        
        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public PrintWriter getLogWriter() throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public int getLoginTimeout() throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
        
        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new SQLException("数据源 [" + name + "] 已禁用");
        }
    }
}
