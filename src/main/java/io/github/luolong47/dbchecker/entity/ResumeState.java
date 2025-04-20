package io.github.luolong47.dbchecker.entity;

import lombok.Data;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 断点续跑状态类，用于保存和恢复处理状态
 */
@Data
public class ResumeState implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // 已完成处理的表名集合
    private Set<String> completedTables = ConcurrentHashMap.newKeySet();
    
    // 保存的处理时间戳
    private long timestamp;
    
    // 任务总数量
    private int totalTables;
    
    // 已完成任务数量
    private int completedCount;
    
    // 额外的状态信息
    private Map<String, Object> extraInfo = new ConcurrentHashMap<>();
} 