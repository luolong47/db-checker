package io.github.luolong47.dbchecker.manager;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程池管理器
 * 统一管理应用中的各种线程池，包括创建和销毁
 */
@Slf4j
@Component
public class ThreadPoolManager implements DisposableBean {

    // 数据库名称列表
    private static final List<String> DB_NAMES = Arrays.asList(
        "ora", "rlcms_base", "rlcms_pv1", "rlcms_pv2", "rlcms_pv3",
        "bscopy_pv1", "bscopy_pv2", "bscopy_pv3"
    );
    // 数据库线程池核心线程数
    private static final int DB_CORE_POOL_SIZE = 2;
    // CSV导出线程池大小
    private static final int CSV_POOL_SIZE = 4;
    // 数据库专用线程池集合，每个数据库一个线程池
    private Map<String, ExecutorService> dbThreadPools;
    // CSV导出专用线程池
    private ExecutorService csvExportExecutor;

    @PostConstruct
    public void init() {
        log.info("初始化线程池管理器...");

        // 初始化数据库专用线程池
        initDbThreadPools();

        // 初始化CSV导出线程池
        initCsvExportExecutor();

        log.info("线程池管理器初始化完成: {}个数据库线程池和1个CSV导出线程池", dbThreadPools.size());
    }

    /**
     * 初始化数据库专用线程池
     */
    private void initDbThreadPools() {
        dbThreadPools = new ConcurrentHashMap<>();

        // 为每个数据库创建一个专用线程池
        for (String dbName : DB_NAMES) {
            ExecutorService dbPool = Executors.newFixedThreadPool(DB_CORE_POOL_SIZE, new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, dbName + "-pool-" + threadNumber.getAndIncrement());
                    if (t.isDaemon()) t.setDaemon(false);
                    if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
                    return t;
                }
            });
            dbThreadPools.put(dbName, dbPool);
            log.debug("创建数据库[{}]专用线程池，核心线程数: {}", dbName, DB_CORE_POOL_SIZE);
        }
    }

    /**
     * 初始化CSV导出线程池
     */
    private void initCsvExportExecutor() {
        csvExportExecutor = Executors.newFixedThreadPool(CSV_POOL_SIZE, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "csv-export-" + threadNumber.getAndIncrement());
                if (t.isDaemon()) t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) t.setPriority(Thread.NORM_PRIORITY);
                return t;
            }
        });
    }

    /**
     * 获取数据库专用线程池
     */
    public ExecutorService getDbThreadPool(String dbName) {
        ExecutorService executor = dbThreadPools.get(dbName);
        if (executor == null) {
            log.warn("数据库[{}]没有专用线程池，将使用公共池", dbName);
            return ForkJoinPool.commonPool();
        }
        return executor;
    }

    /**
     * 获取CSV导出线程池
     */
    public ExecutorService getCsvExportExecutor() {
        return csvExportExecutor;
    }

    /**
     * 获取所有数据库名称
     */
    public List<String> getAllDbNames() {
        return DB_NAMES;
    }

    /**
     * 关闭服务时清理资源
     */
    @Override
    public void destroy() {
        log.info("正在关闭线程池资源...");

        // 关闭CSV导出线程池
        shutdownExecutor(csvExportExecutor, "CSV导出线程池");

        // 关闭所有数据库线程池
        for (Map.Entry<String, ExecutorService> entry : dbThreadPools.entrySet()) {
            shutdownExecutor(entry.getValue(), entry.getKey() + "数据库线程池");
        }

        log.info("所有线程池资源已关闭");
    }

    /**
     * 安全关闭线程池
     */
    private void shutdownExecutor(ExecutorService executor, String poolName) {
        try {
            log.debug("正在关闭{}...", poolName);
            executor.shutdown();
            if (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
                log.warn("{}未能在15秒内完全关闭，将强制关闭", poolName);
                List<Runnable> pendingTasks = executor.shutdownNow();
                log.debug("{}中有{}个任务被取消", poolName, pendingTasks.size());

                // 再次等待，确保任务能完全中止
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.error("{}无法完全关闭，可能导致应用无法正常退出", poolName);
                }
            }
        } catch (InterruptedException e) {
            log.warn("关闭{}时被中断", poolName);
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    /**
     * 强制立即关闭所有线程池
     * 在应用退出前调用这个方法可以确保所有线程都被关闭
     */
    public void shutdownAllNow() {
        log.info("强制关闭所有线程池...");

        // 强制关闭CSV导出线程池
        try {
            csvExportExecutor.shutdownNow();
        } catch (Exception e) {
            log.error("强制关闭CSV导出线程池时出错: {}", e.getMessage());
        }

        // 强制关闭所有数据库线程池
        for (Map.Entry<String, ExecutorService> entry : dbThreadPools.entrySet()) {
            try {
                entry.getValue().shutdownNow();
            } catch (Exception e) {
                log.error("强制关闭{}数据库线程池时出错: {}", entry.getKey(), e.getMessage());
            }
        }

        log.info("已强制关闭所有线程池");
    }
} 