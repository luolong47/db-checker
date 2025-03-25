package io.github.luolong47.dbchecker.task;

import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 断点续跑状态定时保存任务
 */
@Slf4j
@Component
public class ResumeStateTask {

    private final ResumeStateManager resumeStateManager;

    public ResumeStateTask(ResumeStateManager resumeStateManager) {
        this.resumeStateManager = resumeStateManager;
    }

    /**
     * 每2秒执行一次状态保存
     */
    @Scheduled(fixedRate = 2000)
    public void saveResumeState() {
        resumeStateManager.saveResumeStateToFile();
    }
} 