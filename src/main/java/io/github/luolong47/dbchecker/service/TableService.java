package io.github.luolong47.dbchecker.service;

import cn.hutool.extra.spring.SpringUtil;
import com.zaxxer.hikari.HikariDataSource;
import io.github.luolong47.dbchecker.entity.TableEnt;
import io.github.luolong47.dbchecker.manager.DynamicDataSourceManager;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.List;

public interface TableService {

    static TableService getTableService(String dbName){
        DynamicDataSourceManager dataSourceManager = SpringUtil.getBean(DynamicDataSourceManager.class);
        DataSource dataSource = dataSourceManager.getDataSource(dbName);
        if(!(dataSource instanceof HikariDataSource)){
            throw new RuntimeException("数据源不存在");
        }
        String driverClassName = ((HikariDataSource) dataSource).getDriverClassName();
        // 根据驱动类名判断数据库类型并返回对应的TableService
        if (driverClassName.contains("oracle")) {
            return SpringUtil.getBean("oracleTableService", TableService.class);
        } else if (driverClassName.contains("h2")) {
            return SpringUtil.getBean("h2TableService", TableService.class);
        } else if (driverClassName.contains("postgresql") || driverClassName.contains("gaussdb")) {
            return SpringUtil.getBean("gaussDBTableService", TableService.class);
        } else {
            throw new RuntimeException("不支持的数据库类型: " + driverClassName);
        }
    }

    List<TableEnt> getTables(JdbcTemplate jdbcTemplate, List<String> schemas, List<String> tables);
}
