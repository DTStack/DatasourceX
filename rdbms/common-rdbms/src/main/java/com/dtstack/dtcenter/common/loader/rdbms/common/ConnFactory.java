package com.dtstack.dtcenter.common.loader.rdbms.common;

import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:22 2020/1/13
 * @Description：连接工厂
 */
public class ConnFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConnFactory.class);

    protected String driverName = null;

    protected String testSql;

    private AtomicBoolean isFirstLoaded = new AtomicBoolean(true);

    private void init() throws ClassNotFoundException {
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

    public Connection getConn(SourceDTO source) throws Exception {
        init();
        if (StringUtils.isBlank(source.getUsername())) {
            return DriverManager.getConnection(source.getUrl());
        }

        return DriverManager.getConnection(source.getUrl(), source.getProperties());
    }

    public Boolean testConn(SourceDTO source) {
        boolean isConnected = false;
        Connection conn = null;
        try {
            init();
            conn = getConn(source);
            if (StringUtils.isBlank(testSql)) {
                conn.isValid(5);
            } else {
                conn.createStatement().execute((testSql));
            }

            isConnected = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return isConnected;
    }
}
