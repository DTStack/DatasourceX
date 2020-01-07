package com.dtstack.dtcenter.common.loader.rdbms.mysql;

import com.dtstack.dtcenter.loader.constant.ConfigConstant;
import com.dtstack.dtcenter.rdbms.common.RdbmsClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class MysqlClientTest {
    private static final Logger logger = LoggerFactory.getLogger(MysqlClientTest.class);
    private static RdbmsClient rdbsClient = new MysqlClient();

    @Test
    public void getConnFactory() {
        Connection con = null;
        try {
            String url = "jdbc:mysql://172.16.8.109:3306/ide";
            Properties properties = new Properties();
            properties.setProperty(ConfigConstant.USER_NAME, "dtstack");
            properties.setProperty(ConfigConstant.PWD, "abc123");
            con = rdbsClient.getCon(url, properties);
            if (con == null) {
                logger.error("false");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}