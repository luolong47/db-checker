package io.github.luolong47.dbchecker;

import cn.hutool.extra.spring.EnableSpringUtil;
import cn.hutool.extra.spring.SpringUtil;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.entity.TableInfo;
import io.github.luolong47.dbchecker.manager.DynamicDataSourceManager;
import io.github.luolong47.dbchecker.manager.TableManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@Slf4j
@SpringBootApplication
@EnableSpringUtil
public class DbCheckerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbCheckerApplication.class, args);
        // DynamicDataSourceManager dataSourceManager = SpringUtil.getBean(DynamicDataSourceManager.class);
        // dataSourceManager.getAllDataSources().values().forEach(ds->{
        //     if (ds instanceof com.zaxxer.hikari.HikariDataSource) {
        //         com.zaxxer.hikari.HikariDataSource hikariDs = (com.zaxxer.hikari.HikariDataSource) ds;
        //         log.info("连接池名称: {}", hikariDs.getPoolName());
        //         log.info("最大连接数: {}", hikariDs.getMaximumPoolSize());
        //         log.info("最小空闲连接: {}", hikariDs.getMinimumIdle());
        //         log.info("连接超时时间: {}ms", hikariDs.getConnectionTimeout());
        //         log.info("空闲连接超时时间: {}ms", hikariDs.getIdleTimeout());
        //         log.info("连接最大生命周期: {}ms", hikariDs.getMaxLifetime());
        //         log.info("连接泄露检测阈值: {}ms", hikariDs.getLeakDetectionThreshold());
        //         log.info("----------------------------------------");
        //     }
        // });
        TableManager tableManager = SpringUtil.getBean(TableManager.class);
        Map<String, TableInfo> tableInfoMap = tableManager.getTableInfoMap();
        log.info("END");
    }
}
