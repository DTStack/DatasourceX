package com.dtstack.dtcenter.rdbms.common;

import com.dtstack.dtcenter.loader.constant.ConfigConstant;
import com.dtstack.dtcenter.loader.utils.MathUtil;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:54 2020/1/3
 * @Description：连接工程
 */
public class ConnFactory {
    private static final Logger logger = LoggerFactory.getLogger(ConnFactory.class);

    private String dbURL;

    private String userName;

    private String pwd;

    protected String driverName = null;

    protected String testSql = null;

    private AtomicBoolean isFirstLoaded = new AtomicBoolean(true);

    public void init(String url, Properties properties) throws ClassNotFoundException {
        synchronized (ConnFactory.class) {
            if (isFirstLoaded.get()) {
                Class.forName(driverName);
                isFirstLoaded.set(false);
            }

        }
        userName = MathUtil.getString(properties.get(ConfigConstant.USER_NAME));
        pwd = MathUtil.getString(properties.get(ConfigConstant.PWD));
        dbURL = url;

        Preconditions.checkNotNull(url, "db url can't be null");
//        testConn();
    }

    public Connection getConn() throws SQLException {
        Connection conn;
        if (userName == null) {
            conn = DriverManager.getConnection(dbURL);
        } else {
            conn = DriverManager.getConnection(dbURL, userName, pwd);
        }
        return conn;
    }

    public Boolean testConn() {
        boolean isConnected = false;
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            stmt = conn.createStatement();
            stmt.execute(testSql);
            isConnected = true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }

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
