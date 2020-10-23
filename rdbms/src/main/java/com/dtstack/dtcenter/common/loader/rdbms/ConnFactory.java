package com.dtstack.dtcenter.common.loader.rdbms;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:22 2020/1/13
 * @Description：连接工厂
 */
@Slf4j
public class ConnFactory {
    protected String driverName = null;

    protected String testSql;

    private static ConcurrentHashMap<String, Object> hikariDataSources = new ConcurrentHashMap<>();

    private AtomicBoolean isFirstLoaded = new AtomicBoolean(true);

    private static final String CP_POOL_KEY = "url:%s,username:%s,password:%s";

    protected void init() throws ClassNotFoundException {
        // 减少加锁开销
        if (!isFirstLoaded.get()) {
            return;
        }

        synchronized (ConnFactory.class) {
            if (isFirstLoaded.get()) {
                Class.forName(driverName);
                isFirstLoaded.set(false);
            }
        }
    }

    public Connection getConn(ISourceDTO source) throws Exception {
        if (source == null) {
            throw new DtLoaderException("数据源信息为 NULL");
        }
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        boolean isStart = rdbmsSourceDTO.getPoolConfig() != null;
        return isStart && MapUtils.isEmpty(rdbmsSourceDTO.getKerberosConfig()) ?
                getCpConn(rdbmsSourceDTO) : getSimpleConn(rdbmsSourceDTO);
    }

    /**
     * 从连接池获取连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    protected Connection getCpConn(ISourceDTO source) throws Exception {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        String poolKey = getPrimaryKey(rdbmsSourceDTO);
        log.info("获取数据源连接(Hikari), url : {}, userName : {}, kerberosConfig : {}", rdbmsSourceDTO.getUrl(), rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getKerberosConfig());
        HikariDataSource hikariData = (HikariDataSource) hikariDataSources.get(poolKey);
        if (hikariData == null) {
            synchronized (ConnFactory.class) {
                hikariData = (HikariDataSource) hikariDataSources.get(poolKey);
                if (hikariData == null) {
                    hikariData = transHikari(source);
                    hikariDataSources.put(poolKey, hikariData);
                }
            }
        }

        return hikariData.getConnection();
    }

    /**
     * 获取普通连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    protected Connection getSimpleConn(ISourceDTO source) throws Exception {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        init();
        DriverManager.setLoginTimeout(30);
        String url = dealSourceUrl(rdbmsSourceDTO);
        log.info("获取数据源连接, url : {}, userName : {}, kerberosConfig : {}", url, rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getKerberosConfig());
        if (StringUtils.isBlank(rdbmsSourceDTO.getUsername())) {
            return DriverManager.getConnection(url);
        }

        return DriverManager.getConnection(url, rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getPassword());
    }

    /**
     * 处理 URL 地址
     *
     * @param rdbmsSourceDTO
     * @return
     */
    protected String dealSourceUrl(RdbmsSourceDTO rdbmsSourceDTO) {
        return rdbmsSourceDTO.getUrl();
    }

    public Boolean testConn(ISourceDTO source) {
        Connection conn = null;
        Statement statement = null;
        try {
            conn = getConn(source);
            if (StringUtils.isBlank(testSql)) {
                conn.isValid(5);
            } else {
                statement = conn.createStatement();
                statement.execute((testSql));
            }

            return true;
        } catch (Exception e) {
            throw new DtLoaderException("数据源连接异常:" + e.getMessage(), e);
        } finally {
            DBUtil.closeDBResources(null, statement, conn);
        }
    }

    /**
     * sourceDTO 转化为 HikariDataSource
     *
     * @param source
     * @return
     */
    protected HikariDataSource transHikari(ISourceDTO source) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        HikariDataSource hikariData = new HikariDataSource();
        hikariData.setDriverClassName(driverName);
        hikariData.setUsername(rdbmsSourceDTO.getUsername());
        hikariData.setPassword(rdbmsSourceDTO.getPassword());
        hikariData.setJdbcUrl(rdbmsSourceDTO.getUrl());
        hikariData.setConnectionInitSql(testSql);

        hikariData.setConnectionTestQuery(testSql);
        hikariData.setConnectionTimeout(rdbmsSourceDTO.getPoolConfig().getConnectionTimeout());
        hikariData.setIdleTimeout(rdbmsSourceDTO.getPoolConfig().getIdleTimeout());
        hikariData.setMaxLifetime(rdbmsSourceDTO.getPoolConfig().getMaxLifetime());
        hikariData.setMaximumPoolSize(rdbmsSourceDTO.getPoolConfig().getMaximumPoolSize());
        hikariData.setMinimumIdle(rdbmsSourceDTO.getPoolConfig().getMinimumIdle());
        hikariData.setReadOnly(rdbmsSourceDTO.getPoolConfig().getReadOnly());
        return hikariData;
    }

    /**
     * 根据数据源获取数据源唯一 KEY
     *
     * @param source
     * @return
     */
    protected String getPrimaryKey(ISourceDTO source) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        return String.format(CP_POOL_KEY, rdbmsSourceDTO.getUrl(), rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getPassword());
    }
}