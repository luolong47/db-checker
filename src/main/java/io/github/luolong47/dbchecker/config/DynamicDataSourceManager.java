package io.github.luolong47.dbchecker.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 动态数据源管理器
 * 提供统一的数据源获取接口
 */
@Component
public class DynamicDataSourceManager {

    private final Map<String, DataSource> dataSources = new HashMap<>();

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
        
        // 注册所有数据源
        dataSources.put("ora", oraDataSource);
        dataSources.put("ora-slave", oraSlaveDataSource);
        dataSources.put("rlcms-base", rlcmsBaseDataSource);
        dataSources.put("rlcms-pv1", rlcmsPv1DataSource);
        dataSources.put("rlcms-pv2", rlcmsPv2DataSource);
        dataSources.put("rlcms-pv3", rlcmsPv3DataSource);
        dataSources.put("bscopy-pv1", bscopyPv1DataSource);
        dataSources.put("bscopy-pv2", bscopyPv2DataSource);
        dataSources.put("bscopy-pv3", bscopyPv3DataSource);
    }

    /**
     * 获取指定数据源
     * @param name 数据源名称
     * @return 数据源实例
     */
    public DataSource getDataSource(String name) {
        DataSource dataSource = dataSources.get(name);
        if (dataSource == null) {
            throw new IllegalArgumentException("数据源 [" + name + "] 不存在");
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
} 