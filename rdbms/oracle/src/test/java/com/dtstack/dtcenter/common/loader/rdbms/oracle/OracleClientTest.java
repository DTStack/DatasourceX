package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.loader.constant.ConfigConstant;
import com.dtstack.dtcenter.rdbms.common.RdbmsClient;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 13:54 2020/1/6
 * @Description：TODO
 */
public class OracleClientTest {
    private static final Logger logger = LoggerFactory.getLogger(OracleClientTest.class);
    private static RdbmsClient rdbsClient = new OracleClient();

    @Test
    public void getConnFactory() {
        Connection con = null;
        try {
            String url = "jdbc:oracle:thin:@172.16.8.178:1521:xe";
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
