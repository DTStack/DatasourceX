package com.dtstack.dtcenter.loader.rdbms.client;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.constant.ConfigConstant;
import com.dtstack.dtcenter.loader.enums.DataBaseType;
import com.dtstack.dtcenter.loader.service.IRdbmsClient;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

public class RdbmsClientCacheTest {

    private static final RdbmsClientCache clientCache = RdbmsClientCache.getInstance();

    @Test
    public void getMysqlClient() throws Exception {
        IRdbmsClient client = clientCache.getClient(DataBaseType.MySql.name());
        String url = "jdbc:mysql://172.16.8.109:3306/ide";
        Properties properties = new Properties();
        properties.setProperty(ConfigConstant.USER_NAME, "dtstack");
        properties.setProperty(ConfigConstant.PWD, "abc123");
        Connection con = client.getCon(url, properties);
        if (con == null) {
            throw new DtCenterDefException("数据源连接异常");
        }
        con.close();
    }

    @Test
    public void getOracleClient() throws Exception {
        IRdbmsClient client = clientCache.getClient(DataBaseType.Oracle.name());
        String url = "jdbc:oracle:thin:@172.16.8.178:1521:xe";
        Properties properties = new Properties();
        properties.setProperty(ConfigConstant.USER_NAME, "dtstack");
        properties.setProperty(ConfigConstant.PWD, "abc123");
        Connection con = client.getCon(url, properties);
        if (con == null) {
            throw new DtCenterDefException("数据源连接异常");
        }
        con.close();
    }
}