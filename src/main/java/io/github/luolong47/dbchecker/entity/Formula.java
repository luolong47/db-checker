package io.github.luolong47.dbchecker.entity;

import java.math.BigDecimal;

public interface Formula {
    String getName();

    String getDesc();

    boolean result(TableInfo tableInfo,String col);

    BigDecimal diff(TableInfo tableInfo,String col);

    String diffDesc(TableInfo tableInfo,String col);
}
