package io.github.luolong47.dbchecker.service;

import cn.hutool.extra.spring.SpringUtil;
import com.zaxxer.hikari.HikariDataSource;
import io.github.luolong47.dbchecker.entity.TableEnt;
import io.github.luolong47.dbchecker.manager.DynamicDataSourceManager;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;

public abstract class AbstractTableService implements TableService {

}
