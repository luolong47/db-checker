package io.github.luolong47.dbchecker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库初始化脚本配置类
 */
@Component
@ConfigurationProperties(prefix = "spring.datasource.init")
public class DatabaseInitScriptsProperties {

    /**
     * 是否启用初始化
     */
    private boolean enabled = true;

    /**
     * 各数据源脚本配置
     */
    private Map<String, List<String>> scripts = new HashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Map<String, List<String>> getScripts() {
        return scripts;
    }

    public void setScripts(Map<String, List<String>> scripts) {
        this.scripts = scripts;
    }
    
    /**
     * 获取指定数据源的脚本列表
     * @param datasourceName 数据源名称
     * @return 脚本列表
     */
    public List<String> getScriptsForDatasource(String datasourceName) {
        return scripts.get(datasourceName);
    }
} 