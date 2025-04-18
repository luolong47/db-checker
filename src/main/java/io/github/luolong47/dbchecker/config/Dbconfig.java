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
    
    private Export export;
    private Include include;
    private Run run;
    private Resume resume;
    private Rerun rerun;
    private Map<String, Map<String, String>> where;
    private Formula formula;
    private Hints hints;
    private SlaveQuery slaveQuery;
    private Sum sum;
    private Pool pool;

    @Data
    public static class Export {
        private String directory;
    }

    @Data
    public static class Include {
        private String schemas;
        private String tables;
    }

    @Data
    public static class Run {
        private String mode;
    }

    @Data
    public static class Resume {
        private String file;
    }

    @Data
    public static class Rerun {
        private String databases;
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
    public static class Hints {
        private Map<String, String> type;
        private Map<String, String> table;
        private Map<String, String> sql;
    }

    @Data
    public static class SlaveQuery {
        private String tables;
    }

    @Data
    public static class Sum {
        private boolean enable;
        private int minDecimalDigits;
    }

    @Data
    public static class Pool {
        private int defalut;
        private Map<String, Integer> map;
    }
}
