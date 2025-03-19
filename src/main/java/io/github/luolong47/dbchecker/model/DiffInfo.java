package io.github.luolong47.dbchecker.model;

import lombok.Data;

/**
 * 公式计算比较差异信息
 */
@Data
public class DiffInfo {
    private final String description;
    private final String value;
} 