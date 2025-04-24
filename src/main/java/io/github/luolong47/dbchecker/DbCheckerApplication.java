package io.github.luolong47.dbchecker;

import cn.hutool.core.date.StopWatch;
import cn.hutool.extra.spring.EnableSpringUtil;
import cn.hutool.extra.spring.SpringUtil;
import io.github.luolong47.dbchecker.entity.ResumeState;
import io.github.luolong47.dbchecker.manager.ResumeStateManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@SpringBootApplication
@EnableSpringUtil
public class DbCheckerApplication {

    public static void main(String[] args) {
        // 创建并启动全局计时器
        StopWatch globalWatch = new StopWatch("整体程序执行");
        globalWatch.start("程序启动");
        
        SpringApplication.run(DbCheckerApplication.class, args);
        
        // 在启动新任务前先停止当前任务
        globalWatch.stop();

        log.info("程序执行完成，总耗时：\n{}", globalWatch.prettyPrint());
        
        // 输出详细的处理状态摘要
        ResumeStateManager resumeStateManager = SpringUtil.getBean(ResumeStateManager.class);
        ResumeState state = resumeStateManager.getCurrentState();
        
        // 输出已处理的表及其处理时间
        Map<String, Long> tableTimes = state.getTableProcessingTimes();
        if (tableTimes != null && !tableTimes.isEmpty()) {
            log.info("\n-------- 表处理时间排行（前10） --------");
            List<Map.Entry<String, Long>> sortedEntries = new ArrayList<>(tableTimes.entrySet());
            sortedEntries.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));
            
            int count = 0;
            for (Map.Entry<String, Long> entry : sortedEntries) {
                if (count++ >= 10) break;
                log.info("表[{}]: {}ms", entry.getKey(), entry.getValue());
                
                // 显示该表在各数据库的处理时间
                Map<String, Long> dbTimes = state.getTableDbProcessingTimes().get(entry.getKey());
                if (dbTimes != null && !dbTimes.isEmpty()) {
                    String dbTimeStr = dbTimes.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .map(e -> String.format("%s: %dms", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", "));
                    log.info("  └─ 数据库处理时间: {}", dbTimeStr);
                }
            }
        }

        log.info("END");
    }
}
