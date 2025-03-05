package io.github.luolong47.dbchecker.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 数据库服务类，用于测试多数据源
 */
@Slf4j
@Service
public class DatabaseService {

    private final JdbcTemplate primaryJdbcTemplate;
    private final JdbcTemplate secondaryJdbcTemplate;
    private final JdbcTemplate tertiaryJdbcTemplate;

    public DatabaseService(
            @Qualifier("primaryJdbcTemplate") JdbcTemplate primaryJdbcTemplate,
            @Qualifier("secondaryJdbcTemplate") JdbcTemplate secondaryJdbcTemplate,
            @Qualifier("tertiaryJdbcTemplate") JdbcTemplate tertiaryJdbcTemplate) {
        this.primaryJdbcTemplate = primaryJdbcTemplate;
        this.secondaryJdbcTemplate = secondaryJdbcTemplate;
        this.tertiaryJdbcTemplate = tertiaryJdbcTemplate;
    }

    /**
     * 查询主数据源中的用户数据
     */
    public List<Map<String, Object>> queryUsersFromPrimary() {
        String sql = "SELECT * FROM users";
        List<Map<String, Object>> result = primaryJdbcTemplate.queryForList(sql);
        log.info("从主数据源查询到 {} 条用户数据", result.size());
        return result;
    }

    /**
     * 查询第二个数据源中的产品数据
     */
    public List<Map<String, Object>> queryProductsFromSecondary() {
        String sql = "SELECT * FROM products";
        List<Map<String, Object>> result = secondaryJdbcTemplate.queryForList(sql);
        log.info("从第二个数据源查询到 {} 条产品数据", result.size());
        return result;
    }

    /**
     * 查询第三个数据源中的订单数据
     */
    public List<Map<String, Object>> queryOrdersFromTertiary() {
        String sql = "SELECT * FROM orders";
        List<Map<String, Object>> result = tertiaryJdbcTemplate.queryForList(sql);
        log.info("从第三个数据源查询到 {} 条订单数据", result.size());
        return result;
    }
} 