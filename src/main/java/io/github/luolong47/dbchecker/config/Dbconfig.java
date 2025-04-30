package io.github.luolong47.dbchecker.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "db")
public class Dbconfig {
    private Include include = new Include();
    private Export export = new Export();
    private Resume resume = new Resume();
    private Formula formula = new Formula();
    private SlaveQuery slaveQuery = new SlaveQuery();
    private Sum sum = new Sum();
    private Pool pool = new Pool();
    private Map<String, Map<String, String>> where;
    private Hints hints;
    private Init init = new Init();

    @Data
    public static class Pool {
        private ThreadPoolProperties table = new ThreadPoolProperties();
        private ThreadPoolProperties dbQuery = new ThreadPoolProperties();
        private ThreadPoolProperties csvExport = new ThreadPoolProperties();
    }

    @Data
    public static class ThreadPoolProperties {
        private int coreSize = 0;
        private int maxSize = 0;
        private int queueCapacity = 100;
        private long keepAliveTime = 0;
        private String threadNamePrefix = "executor-";
        private String rejectionPolicy = "CALLER_RUNS";
        private boolean useCachedPool = false;
    }
    
    @Data
    public static class Include {
        private String tables;
        private String schemas;
    }

    @Data
    public static class Export {
        private String directory = "./export";
    }

    @Data
    public static class Resume {
        private String file = "./export/resume_state.json";
    }

    @Data
    public static class Formula {
        private String formula1;
        private String formula2;
        private String formula3;
        private String formula4;
        private String formula5;
        private String formula6;
    }

    @Data
    public static class SlaveQuery {
        private String tables;
    }

    @Data
    public static class Sum {
        private boolean enable = true;
        private int minDecimalDigits = 2;
    }

    @Data
    public static class Hints {
        private Map<String, String> type;
        private Map<String, String> table;
        private Map<String, String> sql;
    }

    @Data
    public static class Init {
        private boolean enable = false;
        private Map<String, List<String>> scripts;
    }
}
