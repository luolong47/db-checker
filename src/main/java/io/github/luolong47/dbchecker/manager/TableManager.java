package io.github.luolong47.dbchecker.manager;

import cn.hutool.core.collection.ListUtil;
import io.github.luolong47.dbchecker.config.Dbconfig;
import io.github.luolong47.dbchecker.entity.TableCsvResult;
import io.github.luolong47.dbchecker.entity.TableEnt;
import io.github.luolong47.dbchecker.service.TableService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Slf4j
@Data
@Component
public class TableManager {
    private List<String> tables;
    private List<String> schemas;
    private List<String> dbs = ListUtil.of("ora", "ora-slave", "rlcms-base", "rlcms-pv1", "rlcms-pv2", "rlcms-pv3", "bscopy-pv1", "bscopy-pv2", "bscopy-pv3");
    private Map<String, List<String>> tb2dbs;
    private Map<String, TableService> tableServices;
    private Map<String, TableCsvResult> tableCsvResultMap;
    @Autowired
    private Dbconfig dbconfig;
    @Autowired
    private DynamicJdbcTemplateManager dynamicJdbcTemplateManager;


    
    @PostConstruct
    public void init() {
        // 从db.include.tables配置中初始化表列表
        tables = Optional.ofNullable(dbconfig.getInclude().getTables())
                .filter(s -> !s.trim().isEmpty())
                .map(s -> Arrays.stream(s.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
                
        // 从db.include.schemas配置中初始化schema列表
        schemas = Optional.ofNullable(dbconfig.getInclude().getSchemas())
                .filter(s -> !s.trim().isEmpty())
                .map(s -> Arrays.stream(s.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        initTableServices();
        initTb2dbs();

    }

    private void initTableServices() {
        tableServices = new ConcurrentHashMap<>();
        for (String db : dbs) {
            tableServices.put(db, TableService.getTableService(db));
        }
    }

    private void initTb2dbs() {
        tb2dbs = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (String db : dbs) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                log.info("开始查询数据库 [{}] 中的表信息", db);
                try {
                    JdbcTemplate jdbcTemplate = dynamicJdbcTemplateManager.getJdbcTemplate(db);
                    TableService tableService = tableServices.get(db);
                    List<TableEnt> tables = tableService.getTables(jdbcTemplate, schemas, this.tables);
                    log.info("数据库 [{}] 中查询到 {} 个表", db, tables.size());
                    return tables;
                } catch (Exception e) {
                    log.error("查询数据库 [{}] 中的表信息失败: {}", db, e.getMessage(), e);
                    return ListUtil.<TableEnt>empty();
                }
            }).thenAccept(tableQueryed ->
                tableQueryed.forEach(tableEnt ->
                    tb2dbs.computeIfAbsent(tableEnt.getTableName(), k -> new CopyOnWriteArrayList<>()).add(db)
                )
            );
            
            futures.add(future);
        }
        
        // 等待所有任务完成
        futures.stream()
               .collect(Collectors.collectingAndThen(
                   Collectors.toList(),
                   fs -> CompletableFuture.allOf(fs.toArray(new CompletableFuture[0]))
               ))
               .thenRun(() -> log.info("所有数据库表信息查询完成，共获取到{}个表信息", tb2dbs.size()))
               .exceptionally(e -> {
                   log.error("等待数据库表信息查询时发生错误: {}", e.getMessage(), e);
                   return null;
               })
               .join();
    }

}
