package io.github.luolong47.dbchecker.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Configuration
@EnableConfigurationProperties
public class ExecutorConfig {

    private final Dbconfig dbconfig;

    public ExecutorConfig(Dbconfig dbconfig) {
        this.dbconfig = dbconfig;
    }

    /**
     * 表处理线程池 - 用于处理表级别的并行任务
     * 线程池大小：固定为处理器核心数
     * 队列大小：100，避免无限增长
     * 线程命名前缀：table-executor-
     */
    @Bean
    public ExecutorService tableExecutor() {
        return createThreadPoolExecutor(dbconfig.getPool().getTable(), "表处理线程池");
    }

    /**
     * 数据库查询线程池 - 用于数据库查询操作
     * 线程池大小：处理器核心数 * 2，因为主要是IO操作
     * 队列大小：200，支持较多查询排队
     * 线程命名前缀：db-query-executor-
     */
    @Bean
    public ExecutorService dbQueryExecutor() {
        return createThreadPoolExecutor(dbconfig.getPool().getDbQuery(), "数据库查询线程池");
    }

    /**
     * CSV导出线程池 - 用于处理CSV结果转换和写入
     * 线程池大小：固定为4，避免过多线程同时写文件
     * 队列大小：100，允许足够多的任务等待执行
     * 线程命名前缀：csv-export-executor-
     */
    @Bean
    public ExecutorService csvExportExecutor() {
        return createThreadPoolExecutor(dbconfig.getPool().getCsvExport(), "CSV导出线程池");
    }

    /**
     * 根据配置创建线程池
     * 
     * @param props 线程池配置属性
     * @param poolName 线程池名称，用于日志记录
     * @return 创建的线程池
     */
    private ExecutorService createThreadPoolExecutor(Dbconfig.ThreadPoolProperties props, String poolName) {
        int corePoolSize = props.getCoreSize() > 0 ? props.getCoreSize() : Runtime.getRuntime().availableProcessors();
        int maxPoolSize = props.getMaxSize() > 0 ? props.getMaxSize() : corePoolSize;

        ThreadFactory threadFactory = createThreadFactory(props.getThreadNamePrefix());
        
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            corePoolSize,
            maxPoolSize,
            props.getKeepAliveTime(),
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(props.getQueueCapacity()),
            threadFactory,
            getRejectedExecutionHandler(props.getRejectionPolicy())
        );
        
        logThreadPoolCreation(poolName, executor, props);
        return executor;
    }

    /**
     * 创建具有指定前缀的线程工厂
     */
    private ThreadFactory createThreadFactory(String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, prefix + threadNumber.getAndIncrement());
                thread.setDaemon(true); // 设置为守护线程，防止程序退出时阻塞
                return thread;
            }
        };
    }

    /**
     * 获取线程池拒绝策略处理器
     */
    private RejectedExecutionHandler getRejectedExecutionHandler(String policy) {
        switch (policy.toUpperCase()) {
            case "CALLER_RUNS":
                return new ThreadPoolExecutor.CallerRunsPolicy();
            case "DISCARD":
                return new ThreadPoolExecutor.DiscardPolicy();
            case "DISCARD_OLDEST":
                return new ThreadPoolExecutor.DiscardOldestPolicy();
            case "ABORT":
            default:
                return new ThreadPoolExecutor.AbortPolicy();
        }
    }
    
    /**
     * 记录线程池创建日志
     * 
     * @param poolName 线程池名称
     * @param executor 线程池实例
     * @param props 线程池配置属性
     */
    private void logThreadPoolCreation(String poolName, ThreadPoolExecutor executor, Dbconfig.ThreadPoolProperties props) {
        String rejectionPolicy = props.getRejectionPolicy();
        
        log.info("────────────────────────────────────────────────────────────");
        log.info("成功创建[{}]:", poolName);
        log.info("● 核心线程数: {}", executor.getCorePoolSize());
        log.info("● 最大线程数: {}", executor.getMaximumPoolSize());
        log.info("● 队列类型: {}", executor.getQueue().getClass().getSimpleName());
        log.info("● 队列容量: {}", props.getQueueCapacity());
        log.info("● 线程前缀: {}", props.getThreadNamePrefix());
        log.info("● 拒绝策略: {} ({})", rejectionPolicy, getRejectionPolicyDescription(rejectionPolicy));
        log.info("● 线程空闲时间: {}ms", props.getKeepAliveTime());
        log.info("────────────────────────────────────────────────────────────");
    }
    
    /**
     * 获取拒绝策略的详细描述
     * 
     * @param policy 拒绝策略名称
     * @return 拒绝策略描述
     */
    private String getRejectionPolicyDescription(String policy) {
        switch (policy.toUpperCase()) {
            case "CALLER_RUNS":
                return "由调用线程执行任务，不会丢失数据";
            case "DISCARD":
                return "直接丢弃任务，不会抛出异常";
            case "DISCARD_OLDEST":
                return "丢弃队列最前面的任务，然后重新尝试执行任务";
            case "ABORT":
                return "直接抛出RejectedExecutionException异常";
            default:
                return "未知策略";
        }
    }
}
