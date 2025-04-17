package io.github.luolong47.dbchecker;

import cn.hutool.core.date.StopWatch;
import io.github.luolong47.dbchecker.config.DatabaseInitScriptsProperties;
import io.github.luolong47.dbchecker.config.DbConfig;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import io.github.luolong47.dbchecker.manager.ThreadPoolManager;
import io.github.luolong47.dbchecker.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@EnableConfigurationProperties({DatabaseInitScriptsProperties.class, DbConfig.class})
@EnableScheduling
public class DbCheckerApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(DbCheckerApplication.class, args);

        // 添加JVM关闭钩子，确保线程池正确关闭
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("应用正在关闭，执行最后一次状态持久化...");
            ResumeStateManager resumeStateManager = context.getBean(ResumeStateManager.class);
            resumeStateManager.saveResumeStateToFile();
            
            log.info("应用正在关闭，强制关闭所有线程池...");
            ThreadPoolManager threadPoolManager = context.getBean(ThreadPoolManager.class);
            threadPoolManager.shutdownAllNow();
            log.info("应用已关闭所有线程池，准备退出");
        }));
    }

    /**
     * 应用启动后执行数据库测试
     */
    @Bean
    public CommandLineRunner testDatabases(DatabaseService databaseService, ThreadPoolManager threadPoolManager, ResumeStateManager resumeStateManager) {
        return args -> {
            // 等待一段时间，确保数据库初始化完成
            log.info("等待数据库初始化完成...");
            Thread.sleep(2000);
            
            try {
                // 检查所有数据源连接
                log.info("开始检查所有数据源连接...");
                try {
                    databaseService.checkAllDataSourcesConnection();
                    log.info("所有数据源连接正常，开始执行测试...");
                } catch (Exception e) {
                    log.error("数据源连接检查失败: {}", e.getMessage());
                    // 连接失败直接退出程序
                    log.info("由于数据源连接失败，程序将退出");
                    System.exit(1);
                }
            
                log.info("开始测试多数据源...");
                
                // 使用Stopwatch计时
                StopWatch stopWatch = new StopWatch();
                stopWatch.start("导出金额字段SUM比对");
                
                // 导出金额字段SUM比对结果
                databaseService.exportMoneyFieldSumToCsv();

                // 停止计时并输出耗时
                stopWatch.stop();
                log.info("导出金额字段SUM比对结果完成，总耗时: {} 秒", stopWatch.getLastTaskTimeMillis() / 1000.0);
                log.info("END，当前程序打印END后不会停止");

                // 执行完成后添加退出逻辑
                Thread.sleep(1000); // 给日志输出一点时间
                log.info("程序执行完毕，执行最后一次状态持久化...");
                resumeStateManager.saveResumeStateToFile();
                log.info("状态持久化完成，准备退出...");

                // 直接使用System.exit退出应用
                // Spring容器会在JVM关闭时调用已注册的关闭钩子
                System.exit(0);
            } catch (Exception e) {
                log.error("测试多数据源时发生错误", e);
                // 发生错误时也执行一次状态持久化
                log.info("程序发生错误，执行最后一次状态持久化...");
                resumeStateManager.saveResumeStateToFile();
                // 发生错误也退出
                System.exit(1);
            }
        };
    }
}
