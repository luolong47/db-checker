package io.github.luolong47.dbchecker.model;

import cn.hutool.core.util.StrUtil;
import lombok.Data;

/**
 * 表的唯一标识，由数据库名和表名组成
 */
@Data
public class TableKey {
    /**
     * 数据库名
     */
    private String databaseName;

    /**
     * 表名
     */
    private String tableName;

    public TableKey(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /**
     * 转换为字符串
     * 格式：数据库名|表名
     */
    @Override
    public String toString() {
        return StrUtil.format("{}|{}", databaseName, tableName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableKey tableKey = (TableKey) o;
        return databaseName.equals(tableKey.databaseName) && tableName.equals(tableKey.tableName);
    }

    @Override
    public int hashCode() {
        return 31 * databaseName.hashCode() + tableName.hashCode();
    }
} 