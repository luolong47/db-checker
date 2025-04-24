package io.github.luolong47.dbchecker.manager;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 动态数据源管理器
 * 提供统一的数据源获取接口
 */
@Slf4j
@Component
public class DynamicDataSourceManager {

    private final Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

    @Autowired
    public DynamicDataSourceManager(
            @Qualifier("oraDataSource") DataSource oraDataSource,
            @Qualifier("oraSlaveDataSource") DataSource oraSlaveDataSource,
            @Qualifier("rlcmsBaseDataSource") DataSource rlcmsBaseDataSource,
            @Qualifier("rlcmsPv1DataSource") DataSource rlcmsPv1DataSource,
            @Qualifier("rlcmsPv2DataSource") DataSource rlcmsPv2DataSource,
            @Qualifier("rlcmsPv3DataSource") DataSource rlcmsPv3DataSource,
            @Qualifier("bscopyPv1DataSource") DataSource bscopyPv1DataSource,
            @Qualifier("bscopyPv2DataSource") DataSource bscopyPv2DataSource,
            @Qualifier("bscopyPv3DataSource") DataSource bscopyPv3DataSource) {
        
        // 注册数据源，并检查enable配置
        registerDataSource("ora", oraDataSource);
        registerDataSource("ora-slave", oraSlaveDataSource);
        registerDataSource("rlcms-base", rlcmsBaseDataSource);
        registerDataSource("rlcms-pv1", rlcmsPv1DataSource);
        registerDataSource("rlcms-pv2", rlcmsPv2DataSource);
        registerDataSource("rlcms-pv3", rlcmsPv3DataSource);
        registerDataSource("bscopy-pv1", bscopyPv1DataSource);
        registerDataSource("bscopy-pv2", bscopyPv2DataSource);
        registerDataSource("bscopy-pv3", bscopyPv3DataSource);
    }

    /**
     * 注册数据源，检查enable配置决定是否添加到可用数据源列表
     * @param name 数据源名称
     * @param dataSource 数据源实例
     */
    private void registerDataSource(String name, DataSource dataSource) {
        if (dataSource instanceof HikariDataSource) {
            HikariDataSource hikariDataSource = (HikariDataSource) dataSource;
            
            // 从数据源配置中获取enable属性
            boolean isEnabled = true; // 默认启用
            try {
                // HikariDataSource不直接支持获取配置属性，需要通过反射
                // 尝试通过getDataSourceProperties获取配置
                Object properties = hikariDataSource.getClass().getMethod("getDataSourceProperties").invoke(hikariDataSource);
                if (properties != null) {
                    Object enableValue = properties.getClass().getMethod("getProperty", String.class).invoke(properties, "enable");
                    if (enableValue != null) {
                        isEnabled = Boolean.parseBoolean(enableValue.toString());
                    }
                }
            } catch (Exception e) {
                // 反射异常，使用默认值(true)
                log.debug("无法获取数据源 [{}] 的enable配置，使用默认值true: {}", name, e.getMessage());
            }

            if (isEnabled) {
                dataSources.put(name, dataSource);
                log.info("已注册数据源: [{}]", name);
            } else {
                log.info("数据源 [{}] 已禁用，跳过注册", name);
            }
        } else {
            // 非HikariDataSource直接注册
            dataSources.put(name, dataSource);
            log.info("已注册非HikariCP数据源: [{}]", name);
        }
    }

    /**
     * 获取指定数据源
     * @param name 数据源名称
     * @return 数据源实例
     */
    public DataSource getDataSource(String name) {
        DataSource dataSource = dataSources.get(name);
        if (dataSource == null) {
            throw new IllegalArgumentException("数据源 [" + name + "] 不存在或已禁用");
        }
        return dataSource;
    }

    /**
     * 获取主数据源
     * @return 主数据源实例
     */
    public DataSource getPrimaryDataSource() {
        return getDataSource("ora");
    }

    /**
     * 获取所有数据源名称
     * @return 数据源名称集合
     */
    public Iterable<String> getAllDataSourceNames() {
        return dataSources.keySet();
    }
    
    /**
     * 获取所有数据源
     * @return 数据源Map
     */
    public Map<String, DataSource> getAllDataSources() {
        return new HashMap<>(dataSources);
    }
    
    /**
     * 检查数据源是否存在
     * @param name 数据源名称
     * @return 是否存在
     */
    public boolean hasDataSource(String name) {
        return dataSources.containsKey(name);
    }
} 
