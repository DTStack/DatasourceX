package com.dtstack.dtcenter.common.loader.common;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.utils.DBUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
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

    private AtomicBoolean isFirstLoaded = new AtomicBoolean(true);

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
            throw new DtCenterDefException("数据源信息为 NULL");
        }

        RdbmsSourceDTO rdbmsSourceDTO = (RdbmsSourceDTO) source;

        init();
        DriverManager.setLoginTimeout(5);
        if (StringUtils.isBlank(rdbmsSourceDTO.getUsername())) {
            return DriverManager.getConnection(rdbmsSourceDTO.getUrl());
        }

        return DriverManager.getConnection(rdbmsSourceDTO.getUrl(), rdbmsSourceDTO.getUsername(), rdbmsSourceDTO.getPassword());
    }

    public Boolean testConn(ISourceDTO source) {
        boolean isConnected = false;
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

            isConnected = true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            DBUtil.closeDBResources(null, statement, conn);
        }
        return isConnected;
    }
}
