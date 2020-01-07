package com.dtstack.dtcenter.rdbms.common;

import com.dtstack.dtcenter.loader.service.IRdbmsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:58 2020/1/3
 * @Description：数据库客户端
 */
public abstract class RdbmsClient implements IRdbmsClient {
    private static final Logger logger = LoggerFactory.getLogger(ConnFactory.class);

    private ConnFactory connFactory;

    protected String dbType = "rdbs";

    protected abstract ConnFactory getConnFactory();

    @Override
    public Connection getCon(String url, Properties prop) throws Exception {
        logger.warn("-------get {} connection success-----", dbType);

        connFactory = getConnFactory();
        connFactory.init(url, prop);
        return connFactory.getConn();
    }

    @Override
    public Boolean testCon(String url, Properties prop) {
        return connFactory.testConn();
    }
}
