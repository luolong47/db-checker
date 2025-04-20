package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import io.github.luolong47.dbchecker.config.Dbconfig;
import io.github.luolong47.dbchecker.entity.ResumeState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * 断点续跑状态管理类
 */
@Slf4j
@Component
public class ResumeStateManager {

    @Autowired
    private Dbconfig dbconfig;

    private ResumeState currentState;
    private String stateFilePath;

    /**
     * 初始化状态管理器
     * 
     * @return 如果是断点续跑模式，且成功加载了状态，则返回true；否则返回false
     */
    public boolean init() {
        // 获取状态文件路径
        stateFilePath = Optional.ofNullable(dbconfig.getResume())
            .map(Dbconfig.Resume::getFile)
            .orElse("./export/resume_state.json");

        // 检查运行模式
        String runMode = Optional.ofNullable(dbconfig.getRun())
            .map(Dbconfig.Run::getMode)
            .orElse("FULL");

        // 如果是全量重跑模式，删除状态文件并初始化新状态
        if ("FULL".equalsIgnoreCase(runMode)) {
            deleteStateFile();
            currentState = new ResumeState();
            currentState.setTimestamp(System.currentTimeMillis());
            log.info("全量重跑模式，初始化新的状态");
            return false;
        }

        // 如果是指定数据库重跑模式，也重新初始化状态
        if ("DB".equalsIgnoreCase(runMode)) {
            deleteStateFile();
            currentState = new ResumeState();
            currentState.setTimestamp(System.currentTimeMillis());
            log.info("指定数据库重跑模式，初始化新的状态");
            return false;
        }

        // 尝试加载状态文件
        if (loadState()) {
            log.info("断点续跑模式，成功加载状态文件，已完成 {} 个表的处理",
                    currentState.getCompletedCount());
            return true;
        } else {
            // 加载失败，初始化新状态
            currentState = new ResumeState();
            currentState.setTimestamp(System.currentTimeMillis());
            log.info("断点续跑模式，但未找到有效的状态文件，将创建新状态");
            return false;
        }
    }

    /**
     * 加载状态文件
     * 
     * @return 加载成功返回true，失败返回false
     */
    private boolean loadState() {
        try {
            File stateFile = new File(stateFilePath);
            if (!stateFile.exists() || stateFile.length() == 0) {
                log.warn("状态文件不存在或为空: {}", stateFilePath);
                return false;
            }

            String jsonStr = FileUtil.readUtf8String(stateFile);
            currentState = JSONUtil.toBean(jsonStr, ResumeState.class);
            log.info("成功从文件加载状态: {}", stateFilePath);
            return true;
        } catch (Exception e) {
            log.error("加载状态文件失败: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 删除状态文件
     */
    private void deleteStateFile() {
        try {
            File stateFile = new File(stateFilePath);
            if (stateFile.exists()) {
                if (stateFile.delete()) {
                    log.info("已删除旧的状态文件: {}", stateFilePath);
                } else {
                    log.warn("无法删除状态文件: {}", stateFilePath);
                }
            }
        } catch (Exception e) {
            log.error("删除状态文件时发生错误: {}", e.getMessage(), e);
        }
    }

    /**
     * 保存当前状态到文件
     */
    public void saveState() {
        try {
            // 确保目录存在
            Path dirPath = Paths.get(stateFilePath).getParent();
            if (dirPath != null && !Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            // 更新时间戳
            currentState.setTimestamp(System.currentTimeMillis());
            
            // 将对象转换为JSON并写入文件
            String jsonStr = JSONUtil.toJsonStr(currentState);
            FileUtil.writeUtf8String(jsonStr, stateFilePath);
            log.debug("已保存状态到文件: {}", stateFilePath);
        } catch (Exception e) {
            log.error("保存状态文件失败: {}", e.getMessage(), e);
        }
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

        currentState.getCompletedTables().add(tableName);
        currentState.setCompletedCount(currentState.getCompletedTables().size());
        currentState.setTotalTables(totalTables);
        
        // 每完成一个表就保存一次状态
        saveState();
        
        log.debug("表 [{}] 已标记为完成，当前进度: {}/{}",
                tableName, currentState.getCompletedCount(), totalTables);
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

    /**
     * 获取当前状态
     * 
     * @return 当前状态对象
     */
    public ResumeState getCurrentState() {
        return currentState;
    }
} 
