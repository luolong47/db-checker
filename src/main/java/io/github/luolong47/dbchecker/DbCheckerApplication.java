package io.github.luolong47.dbchecker;

import io.github.luolong47.dbchecker.config.DatabaseInitScriptsProperties;
import io.github.luolong47.dbchecker.config.DbWhereConditionConfig;
import io.github.luolong47.dbchecker.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.util.StopWatch;

@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableConfigurationProperties({DatabaseInitScriptsProperties.class, DbWhereConditionConfig.class})
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

                // 使用Stopwatch计时
                StopWatch stopWatch = new StopWatch();
                stopWatch.start("导出金额字段SUM比对");
                
                // 导出金额字段SUM比对结果
                databaseService.exportMoneyFieldSumToExcel();

                // 停止计时并输出耗时
                stopWatch.stop();
                log.info("导出金额字段SUM比对结果完成，总耗时: {} 秒", stopWatch.getTotalTimeSeconds());
                log.info("END");
            } catch (Exception e) {
                log.error("测试多数据源时发生错误", e);
            }
        };
    }
}
