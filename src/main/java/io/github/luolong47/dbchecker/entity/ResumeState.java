package io.github.luolong47.dbchecker.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 断点续跑状态类，用于保存和恢复处理状态
 */
@Data
public class ResumeState implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // 已完成处理的表名集合
    private Set<String> completedTables = ConcurrentHashMap.newKeySet();
    
    // 进行中的表名集合
    private Set<String> processingTables = ConcurrentHashMap.newKeySet();
    
    // 待处理的表名集合
    private Set<String> pendingTables = ConcurrentHashMap.newKeySet();
    
    // 保存的处理时间戳
    private AtomicLong timestamp = new AtomicLong(0);
    
    // 任务总数量
    private AtomicInteger totalTables = new AtomicInteger(0);
    
    // 已完成任务数量
    private AtomicInteger completedCount = new AtomicInteger(0);
    
    // 进行中的任务数量
    private AtomicInteger processingCount = new AtomicInteger(0);
    
    // 待处理的任务数量
    private AtomicInteger pendingCount = new AtomicInteger(0);
    
    // 当前进度百分比
    private AtomicReference<Double> progressPercentage = new AtomicReference<>(0.0);
    
    // 已处理的每张表的总体用时（表名 -> 总毫秒数）
    private Map<String, AtomicLong> tableProcessingTimes = new ConcurrentHashMap<>();
    
    // 已处理的每张表在每个库的处理用时（表名 -> {库名 -> 毫秒数}）
    private Map<String, Map<String, AtomicLong>> tableDbProcessingTimes = new ConcurrentHashMap<>();
    
    // 额外的状态信息
    private Map<String, Object> extraInfo = new ConcurrentHashMap<>();
    
    /**
     * 更新进度百分比
     */
    public void updateProgressPercentage() {
        if (totalTables.get() > 0) {
            double progress = (double) completedCount.get() / totalTables.get() * 100;
            progressPercentage.set(progress);
        } else {
            progressPercentage.set(0.0);
        }
    }
    
    /**
     * 记录表处理总时间
     * 
     * @param tableName 表名
     * @param timeMillis 处理时间（毫秒）
     */
    public void recordTableProcessingTime(String tableName, long timeMillis) {
        tableProcessingTimes.computeIfAbsent(tableName, k -> new AtomicLong(0))
            .set(timeMillis);
    }
    
    /**
     * 记录表在特定数据库的处理时间
     * 
     * @param tableName 表名
     * @param dbName 数据库名
     * @param timeMillis 处理时间（毫秒）
     */
    public void recordTableDbProcessingTime(String tableName, String dbName, long timeMillis) {
        tableDbProcessingTimes
            .computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(dbName, k -> new AtomicLong(0))
            .set(timeMillis);
    }
    
    // 自定义getter方法，让外部调用更方便
    public int getTotalTables() {
        return totalTables.get();
    }
    
    public void setTotalTables(int totalTables) {
        this.totalTables.set(totalTables);
    }
    
    public int getCompletedCount() {
        return completedCount.get();
    }
    
    public void setCompletedCount(int completedCount) {
        this.completedCount.set(completedCount);
    }
    
    public int getProcessingCount() {
        return processingCount.get();
    }
    
    public void setProcessingCount(int processingCount) {
        this.processingCount.set(processingCount);
    }
    
    public int getPendingCount() {
        return pendingCount.get();
    }
    
    public void setPendingCount(int pendingCount) {
        this.pendingCount.set(pendingCount);
    }
    
    public long getTimestamp() {
        return timestamp.get();
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp.set(timestamp);
    }
    
    public double getProgressPercentage() {
        return progressPercentage.get();
    }
    
    public Map<String, Long> getTableProcessingTimes() {
        Map<String, Long> result = new ConcurrentHashMap<>();
        tableProcessingTimes.forEach((tableName, atomicLong) -> 
            result.put(tableName, atomicLong.get()));
        return result;
    }
    
    public Map<String, Map<String, Long>> getTableDbProcessingTimes() {
        Map<String, Map<String, Long>> result = new ConcurrentHashMap<>();
        tableDbProcessingTimes.forEach((tableName, dbTimeMap) -> {
            Map<String, Long> dbTimes = new ConcurrentHashMap<>();
            dbTimeMap.forEach((dbName, atomicLong) -> 
                dbTimes.put(dbName, atomicLong.get()));
            result.put(tableName, dbTimes);
        });
        return result;
    }
} 
