package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.date.StopWatch;
import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.config.Dbconfig;
import io.github.luolong47.dbchecker.entity.ResumeState;
import io.github.luolong47.dbchecker.entity.TableInfo;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 断点续跑状态管理类
 */
@Slf4j
@Component
public class ResumeStateManager {

    @Resource
    private Dbconfig dbconfig;

    /**
     *  获取当前状态
     */
    @Getter
    private ResumeState currentState;
    private File stateFile;
    
    // 记录每个表的StopWatch，用于统计处理时间
    final private Map<String, StopWatch> tableStopWatches = new ConcurrentHashMap<>();
    
    // 用于异步保存状态的线程池
    private final ExecutorService saveStateExecutor = Executors.newSingleThreadExecutor();
    
    // 用于保护状态保存的锁
    private final ReentrantLock saveStateLock = new ReentrantLock();
    
    // 保存的TableInfo数据
    private Map<String, TableInfo> lastTableInfoMap;

    /**
     * 初始化状态管理器
     * 
     * @return 如果是断点续跑模式，且成功加载了状态，则返回true；否则返回false
     */
    public boolean init() {
        try {
            // 获取状态文件名
            String stateFileName = Optional.ofNullable(dbconfig.getResume())
                .map(Dbconfig.Resume::getFile)
                .orElse("resume_state.json");
                
            // 创建File对象
            stateFile = new File(stateFileName);
            FileUtil.mkParentDirs(stateFile);

            // 检查运行模式
            String runMode = Optional.ofNullable(dbconfig.getRun())
                .map(Dbconfig.Run::getMode)
                .orElse("FULL");

            // 如果是全量重跑模式，删除状态文件并初始化新状态
            if ("FULL".equalsIgnoreCase(runMode)) {
                FileUtil.del(stateFile);
                currentState = new ResumeState();
                currentState.setTimestamp(System.currentTimeMillis());
                log.info("全量重跑模式，初始化新的状态");
                return false;
            }

            // 如果是指定数据库重跑模式，也重新初始化状态
            if ("DB".equalsIgnoreCase(runMode)) {
                FileUtil.del(stateFile);
                currentState = new ResumeState();
                currentState.setTimestamp(System.currentTimeMillis());
                log.info("指定数据库重跑模式，初始化新的状态");
                return false;
            }

            // 尝试加载状态文件
            if (loadState()) {
                log.info("断点续跑模式，成功加载状态文件，已完成 {} 个表的处理，进度 {}%",
                        currentState.getCompletedCount(), 
                        String.format("%.2f", currentState.getProgressPercentage()));
                return true;
            } else {
                // 加载失败，初始化新状态
                currentState = new ResumeState();
                currentState.setTimestamp(System.currentTimeMillis());
                log.info("断点续跑模式，但未找到有效的状态文件，将创建新状态");
                return false;
            }
        } catch (Exception e) {
            log.error("初始化状态管理器时发生错误: {}", e.getMessage(), e);
            currentState = new ResumeState();
            currentState.setTimestamp(System.currentTimeMillis());
            return false;
        }
    }

    /**
     * 初始化表列表
     * 
     * @param allTables 所有表的列表
     * @param totalTables 表的总数量
     */
    public void initTableLists(Set<String> allTables, int totalTables) {
        if (currentState == null) {
            currentState = new ResumeState();
        }
        
        currentState.setTotalTables(totalTables);
        
        // 设置待处理的表列表：所有表减去已完成的表
        Set<String> pendingTables = ConcurrentHashMap.newKeySet();
        pendingTables.addAll(allTables);
        pendingTables.removeAll(currentState.getCompletedTables());
        pendingTables.removeAll(currentState.getProcessingTables());
        
        currentState.getPendingTables().clear();
        currentState.getPendingTables().addAll(pendingTables);
        currentState.setPendingCount(pendingTables.size());
        
        // 更新进度
        currentState.updateProgressPercentage();
        
        log.info("初始化表列表完成: 总数量={}, 已完成={}, 进行中={}, 待处理={}",
            totalTables, 
            currentState.getCompletedCount(),
            currentState.getProcessingCount(),
            currentState.getPendingCount());
        
        // 保存状态
        saveState();
    }

    /**
     * 将表标记为进行中状态
     * 
     * @param tableName 表名
     */
    public void markTableProcessing(String tableName) {
        if (currentState == null) {
            currentState = new ResumeState();
        }
        
        if (!currentState.getProcessingTables().contains(tableName) && 
            !currentState.getCompletedTables().contains(tableName)) {
            // 从待处理表中移除
            currentState.getPendingTables().remove(tableName);
            
            // 添加到进行中表
            currentState.getProcessingTables().add(tableName);
            
            // 更新计数
            currentState.setPendingCount(currentState.getPendingTables().size());
            currentState.setProcessingCount(currentState.getProcessingTables().size());
            
            // 创建并启动表的计时器
            StopWatch tableWatch = new StopWatch("表[" + tableName + "]处理计时");
            tableWatch.start("总体处理");
            tableStopWatches.put(tableName, tableWatch);
            
            // 更新进度
            currentState.updateProgressPercentage();
            
            // 保存状态
            saveState();
            
            log.debug("表[{}]已标记为进行中，当前进度: {}%, 进行中: {}, 待处理: {}",
                tableName, 
                String.format("%.2f", currentState.getProgressPercentage()),
                currentState.getProcessingCount(),
                currentState.getPendingCount());
        }
    }
    
    /**
     * 记录表在数据库的处理时间
     * 
     * @param tableName 表名
     * @param dbName 数据库名
     * @param timeMillis 处理时间（毫秒）
     */
    public void recordTableDbTime(String tableName, String dbName, long timeMillis) {
        if (currentState == null) {
            return;
        }
        
        currentState.recordTableDbProcessingTime(tableName, dbName, timeMillis);
        log.debug("记录表[{}]在数据库[{}]的处理时间: {}ms", tableName, dbName, timeMillis);
        
        // 每记录一次时间就保存状态
        saveState();
    }

    /**
     * 将表标记为已完成
     * 
     * @param tableName 表名
     * @param totalTables 总表数
     */
    public void markTableCompleted(String tableName, int totalTables) {
        if (currentState == null) {
            currentState = new ResumeState();
        }

        // 停止并记录表的处理时间
        StopWatch tableWatch = tableStopWatches.remove(tableName);
        if (tableWatch != null && tableWatch.isRunning()) {
            tableWatch.stop();
            long totalTimeMillis = tableWatch.getLastTaskTimeMillis();
            currentState.recordTableProcessingTime(tableName, totalTimeMillis);
            log.debug("表[{}]总处理时间: {}ms", tableName, totalTimeMillis);
        }
        
        // 从进行中表中移除
        currentState.getProcessingTables().remove(tableName);
        
        // 添加到已完成表
        currentState.getCompletedTables().add(tableName);
        
        // 更新计数
        currentState.setCompletedCount(currentState.getCompletedTables().size());
        currentState.setProcessingCount(currentState.getProcessingTables().size());
        currentState.setTotalTables(totalTables);
        
        // 更新进度
        currentState.updateProgressPercentage();
        
        // 每完成一个表就保存一次状态
        saveState();
        
        log.debug("表[{}]已标记为完成，当前进度: {}%, 已完成: {}/{}, 进行中: {}",
                tableName, 
                String.format("%.2f", currentState.getProgressPercentage()),
                currentState.getCompletedCount(), 
                totalTables,
                currentState.getProcessingCount());
    }

    /**
     * 加载状态文件
     * 
     * @return 加载成功返回true，失败返回false
     */
    private boolean loadState() {
        try {
            if (!FileUtil.exist(stateFile) || FileUtil.size(stateFile) == 0) {
                log.warn("状态文件不存在或为空: {}", stateFile.getAbsolutePath());
                return false;
            }

            String jsonStr = FileUtil.readUtf8String(stateFile);
            
            // 使用自定义方法处理原子类型的反序列化
            JSONObject jsonObject = JSONUtil.parseObj(jsonStr);
            currentState = new ResumeState();
            
            // 设置基本属性
            if (jsonObject.containsKey("completedTables")) {
                currentState.getCompletedTables().addAll(jsonObject.getJSONArray("completedTables").toList(String.class));
            }
            if (jsonObject.containsKey("processingTables")) {
                currentState.getProcessingTables().addAll(jsonObject.getJSONArray("processingTables").toList(String.class));
            }
            if (jsonObject.containsKey("pendingTables")) {
                currentState.getPendingTables().addAll(jsonObject.getJSONArray("pendingTables").toList(String.class));
            }
            
            // 设置原子类型属性
            if (jsonObject.containsKey("timestamp")) {
                currentState.setTimestamp(jsonObject.getLong("timestamp"));
            }
            if (jsonObject.containsKey("totalTables")) {
                currentState.setTotalTables(jsonObject.getInt("totalTables"));
            }
            if (jsonObject.containsKey("completedCount")) {
                currentState.setCompletedCount(jsonObject.getInt("completedCount"));
            }
            if (jsonObject.containsKey("processingCount")) {
                currentState.setProcessingCount(jsonObject.getInt("processingCount"));
            }
            if (jsonObject.containsKey("pendingCount")) {
                currentState.setPendingCount(jsonObject.getInt("pendingCount"));
            }
            
            // 设置表处理时间
            if (jsonObject.containsKey("tableProcessingTimes")) {
                JSONObject timesJson = jsonObject.getJSONObject("tableProcessingTimes");
                timesJson.forEach((tableName, value) -> {
                    if (value instanceof Number) {
                        currentState.recordTableProcessingTime(tableName, ((Number) value).longValue());
                    }
                });
            }
            
            // 设置数据库处理时间
            if (jsonObject.containsKey("tableDbProcessingTimes")) {
                JSONObject dbTimesJson = jsonObject.getJSONObject("tableDbProcessingTimes");
                dbTimesJson.forEach((tableName, dbTimesObj) -> {
                    if (dbTimesObj instanceof JSONObject) {
                        JSONObject dbTimes = (JSONObject) dbTimesObj;
                        dbTimes.forEach((dbName, timeValue) -> {
                            if (timeValue instanceof Number) {
                                currentState.recordTableDbProcessingTime(
                                    tableName, dbName, ((Number) timeValue).longValue());
                            }
                        });
                    }
                });
            }
            
            // 从持久化状态恢复后，确保计数一致
            currentState.setCompletedCount(currentState.getCompletedTables().size());
            currentState.setProcessingCount(currentState.getProcessingTables().size());
            currentState.setPendingCount(currentState.getPendingTables().size());
            currentState.updateProgressPercentage();
            
            log.info("成功从文件加载状态: {}", stateFile.getAbsolutePath());
            return true;
        } catch (Exception e) {
            log.error("加载状态文件失败: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 保存当前状态到文件
     */
    public void saveState() {
        saveState(lastTableInfoMap);
    }
    
    /**
     * 保存当前状态到文件，包含tableInfoMap数据
     * 
     * @param tableInfoMap 表信息映射
     */
    public void saveState(Map<String, TableInfo> tableInfoMap) {
        // 保存最新的tableInfoMap引用
        if (tableInfoMap != null) {
            this.lastTableInfoMap = tableInfoMap;
        }
        
        // 异步执行保存操作
        saveStateExecutor.submit(() -> {
            // 加锁确保线程安全
            saveStateLock.lock();
            try {
                // 更新时间戳
                currentState.setTimestamp(System.currentTimeMillis());
                
                // 创建用于序列化的JSON对象
                JSONObject jsonObject = new JSONObject();
                
                // 添加基本集合属性
                jsonObject.set("completedTables", currentState.getCompletedTables());
                jsonObject.set("processingTables", currentState.getProcessingTables());
                jsonObject.set("pendingTables", currentState.getPendingTables());
                
                // 添加原子类型属性
                jsonObject.set("timestamp", currentState.getTimestamp());
                jsonObject.set("totalTables", currentState.getTotalTables());
                jsonObject.set("completedCount", currentState.getCompletedCount());
                jsonObject.set("processingCount", currentState.getProcessingCount());
                jsonObject.set("pendingCount", currentState.getPendingCount());
                jsonObject.set("progressPercentage", currentState.getProgressPercentage());
                
                // 添加表处理时间
                JSONObject tableTimesJson = new JSONObject();
                currentState.getTableProcessingTimes().forEach(tableTimesJson::set);
                jsonObject.set("tableProcessingTimes", tableTimesJson);
                
                // 添加数据库处理时间
                JSONObject dbTimesJson = new JSONObject();
                currentState.getTableDbProcessingTimes().forEach((tableName, dbTimes) -> {
                    JSONObject dbTimesObj = new JSONObject();
                    dbTimes.forEach(dbTimesObj::set);
                    dbTimesJson.set(tableName, dbTimesObj);
                });
                jsonObject.set("tableDbProcessingTimes", dbTimesJson);
                
                // 添加TableInfoMap数据，如果有的话
                if (lastTableInfoMap != null && !lastTableInfoMap.isEmpty()) {
                    JSONObject tableInfoJson = new JSONObject();
                    lastTableInfoMap.forEach((tableName, tableInfo) -> {
                        if (tableInfo != null) {
                            JSONObject infoJson = new JSONObject();
                            infoJson.set("tableName", tableInfo.getTableName());
                            infoJson.set("dbs", tableInfo.getDbs());
                            
                            // 添加求和结果
                            if (tableInfo.getSumResult() != null) {
                                JSONObject sumResultJson = new JSONObject();
                                tableInfo.getSumResult().forEach((col, dbValues) -> {
                                    JSONObject dbValuesJson = new JSONObject();
                                    dbValues.forEach(dbValuesJson::set);
                                    sumResultJson.set(col, dbValuesJson);
                                });
                                infoJson.set("sumResult", sumResultJson);
                            }
                            
                            // 添加求和列
                            if (tableInfo.getSumCols() != null) {
                                infoJson.set("sumCols", tableInfo.getSumCols());
                            }
                            
                            // 添加公式信息
                            if (tableInfo.getFormula() != null) {
                                infoJson.set("formulaDesc", tableInfo.getFormula().getDesc());
                            }
                            
                            tableInfoJson.set(tableName, infoJson);
                        }
                    });
                    jsonObject.set("tableInfoMap", tableInfoJson);
                }
                
                // 将对象转换为JSON并写入文件
                String jsonStr = jsonObject.toString();
                
                FileUtil.writeUtf8String(jsonStr, stateFile);
                
                log.info("已异步保存状态到文件: {}", stateFile.getAbsolutePath());
            } catch (Exception e) {
                log.error("保存状态文件失败: {}", e.getMessage(), e);
            } finally {
                saveStateLock.unlock();
            }
        });
    }
    
    /**
     * 关闭状态管理器资源
     */
    public void shutdown() {
        log.info("关闭状态管理器资源...");
        saveStateExecutor.shutdown();
    }

    /**
     * 检查表是否已完成处理
     * 
     * @param tableName 表名
     * @return 如果表已处理完成，则返回true；否则返回false
     */
    public boolean isTableCompleted(String tableName) {
        return currentState != null && currentState.getCompletedTables().contains(tableName);
    }

}
