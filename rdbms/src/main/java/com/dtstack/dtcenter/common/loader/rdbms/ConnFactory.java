/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.rdbms;

import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.common.utils.PropertiesUtil;
import com.dtstack.dtcenter.common.loader.common.utils.ReflectUtil;
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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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

    // 错误匹配规则类，需要各个数据源去实现该规则接口并在创建连接工厂时初始化该成员变量
    protected IErrorPattern errorPattern = null;

    // 异常适配器
    protected final IErrorAdapter errorAdapter = new ErrorAdapterImpl();

    protected String testSql;

    private static ConcurrentHashMap<String, Object> hikariDataSources = new ConcurrentHashMap<>();

    private AtomicBoolean isFirstLoaded = new AtomicBoolean(true);

    private static final String CP_POOL_KEY = "url:%s,username:%s,password:%s,properties:%s";

    /**
     * 线程池 - 用于部分数据源获取连接超时处理
     */
    protected static ExecutorService executor = new ThreadPoolExecutor(5, 10, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<>(1000), new DtClassThreadFactory("connFactory"));

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

    /**
     * 获取连接，对抛出异常进行统一处理
     *
     * @param iSource
     * @return
     * @throws Exception
     */
    @Deprecated
    public Connection getConn(ISourceDTO iSource) throws Exception {
        return getConn(iSource, StringUtils.EMPTY);
    }

    /**
     * 获取连接，对抛出异常进行统一处理
     *
     * @param iSource
     * @param taskParams
     * @return
     * @throws Exception
     */
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        if (iSource == null) {
            throw new DtLoaderException(" source is null");
        }
        try {
            RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) iSource;
            boolean isStart = rdbmsSourceDTO.getPoolConfig() != null;
             Connection connection = isStart && MapUtils.isEmpty(rdbmsSourceDTO.getKerberosConfig()) ?
                    getCpConn(rdbmsSourceDTO) : getSimpleConn(rdbmsSourceDTO);

            return setSchema(connection, rdbmsSourceDTO.getSchema());
        } catch (Exception e){
            // 对异常进行统一处理
            throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
        }
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
        log.info("datasource connected(Hikari), url : {}, userName : {}, kerberosConfig : {}", rdbmsSourceDTO.getUrl(), rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getKerberosConfig());
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
        log.info("datasource connected, url : {}, userName : {}, kerberosConfig : {}", url, rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getKerberosConfig());
        return DriverManager.getConnection(url, PropertiesUtil.convertToProp(rdbmsSourceDTO));
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
            conn = getConn(source, StringUtils.EMPTY);
            if (StringUtils.isBlank(testSql)) {
                conn.isValid(5);
            } else {
                statement = conn.createStatement();
                statement.execute((testSql));
            }
            return true;
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }finally {
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

        // 设置 driverClassName
        String driverClassName = getDriverClassName(source);
        hikariData.setDriverClassName(driverClassName);
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

        Properties properties = PropertiesUtil.convertToProp(rdbmsSourceDTO);
        for (Object key : properties.keySet()) {
            hikariData.addDataSourceProperty(key.toString(), properties.get(key));
        }
        return hikariData;
    }

    protected String getDriverClassName(ISourceDTO source) {
        return driverName;
    }

    /**
     * 根据数据源获取数据源唯一 KEY
     *
     * @param source
     * @return
     */
    protected String getPrimaryKey(ISourceDTO source) {
        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;
        String properties = ReflectUtil.fieldExists(RdbmsSourceDTO.class, "properties") ? rdbmsSourceDTO.getProperties() : "";
        return String.format(CP_POOL_KEY, rdbmsSourceDTO.getUrl(), rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getPassword(), properties);
    }

    public Connection setSchema(Connection conn, String schema) {
        if (StringUtils.isBlank(schema)) {
            return conn;
        }
        try {
            conn.setSchema(schema);
        } catch (Throwable e) {
            log.warn(String.format("setting schema exception : %s", e.getMessage()), e);
        }
        return conn;
    }
}
