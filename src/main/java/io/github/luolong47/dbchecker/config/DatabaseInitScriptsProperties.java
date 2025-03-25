package io.github.luolong47.dbchecker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库初始化脚本配置类
 */
@ConfigurationProperties(prefix = "spring.datasource.init")
@Data
public class DatabaseInitScriptsProperties {

    /**
     * 是否启用初始化
     */
    private boolean enabled = true;

    /**
     * 各数据源脚本配置
     */
    private Map<String, List<String>> scripts = new HashMap<>();

} 