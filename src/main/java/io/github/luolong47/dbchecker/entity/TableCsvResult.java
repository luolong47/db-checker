package io.github.luolong47.dbchecker.entity;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class TableCsvResult {
    private String tableName;
    private String dbs;
    private String sumCols;
    private String col;
    private BigDecimal sumOraAll;
    private BigDecimal sumOra;
    private BigDecimal sumRlcmsBase;
    private BigDecimal sumRlcmsPv1;
    private BigDecimal sumRlcmsPv2;
    private BigDecimal sumRlcmsPv3;
    private BigDecimal sumBscopyPv1;
    private BigDecimal sumBscopyPv2;
    private BigDecimal sumBscopyPv3;
    private String formula;
    private String formulaResult;
    private BigDecimal diff;
    private String diffDesc;
}
