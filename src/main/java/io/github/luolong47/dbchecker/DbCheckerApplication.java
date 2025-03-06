package io.github.luolong47.dbchecker;

import io.github.luolong47.dbchecker.config.DatabaseInitScriptsProperties;
import io.github.luolong47.dbchecker.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Map;

@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableConfigurationProperties(DatabaseInitScriptsProperties.class)
public class DbCheckerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DbCheckerApplication.class, args);
    }

    /**
     * 应用启动后执行数据库测试
     */
    @Bean
    public CommandLineRunner testDatabases(DatabaseService databaseService) {
        return args -> {
            // 等待一段时间，确保数据库初始化完成
            log.info("等待数据库初始化完成...");
            Thread.sleep(2000);
            
            log.info("开始测试多数据源...");
            
            try {
                // 测试主数据源
                log.info("===== 测试主数据源 =====");
                List<Map<String, Object>> users = databaseService.queryUsersFromPrimary();
                users.forEach(user -> log.info("用户: {}", user));
                
                // 测试第二个数据源
                log.info("===== 测试第二个数据源 =====");
                List<Map<String, Object>> products = databaseService.queryProductsFromSecondary();
                products.forEach(product -> log.info("产品: {}", product));
                
                // 测试第三个数据源
                log.info("===== 测试第三个数据源 =====");
                List<Map<String, Object>> orders = databaseService.queryOrdersFromTertiary();
                orders.forEach(order -> log.info("订单: {}", order));
                
                log.info("多数据源测试完成！");
                
                // 导出金额字段SUM比对结果
                databaseService.exportMoneyFieldSumToExcel();
                
                log.info("END");
            } catch (Exception e) {
                log.error("测试多数据源时发生错误", e);
            }
        };
    }
}
