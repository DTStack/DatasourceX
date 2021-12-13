package com.dtstack.dtcenter.common.loader.hive3;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.Hive3CDPSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：qianyi
 * @Date ：Created in 14:03 2021/05/13
 * @Description：Hive 连接池工厂
 */
@Slf4j
public class HiveConnFactory extends ConnFactory {
    // Hive 属性前缀
    private static final String HIVE_CONF_PREFIX = "hiveconf:";

    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE3.getDriverClassName();
        this.errorPattern = new HiveErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        init();
        Hive3CDPSourceDTO hive3CDPSourceDTO = (Hive3CDPSourceDTO) iSource;

        Connection connection = KerberosLoginUtil.loginWithUGI(hive3CDPSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        Properties properties = DBUtil.stringToProperties(taskParams);
                        // 特殊处理 properties 属性
                        dealProperties(properties);
                        properties.put(DtClassConsistent.PublicConsistent.USER, hive3CDPSourceDTO.getUsername() == null ? "" : hive3CDPSourceDTO.getUsername());
                        properties.put(DtClassConsistent.PublicConsistent.PASSWORD, hive3CDPSourceDTO.getPassword() == null ? "" : hive3CDPSourceDTO.getPassword());
                        String urlWithoutSchema = HiveDriverUtil.removeSchema(hive3CDPSourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, properties);
                    } catch (Exception e) {
                        // 对异常进行统一处理
                        throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
                    }
                }
        );
        openSessionDbTxnManager(connection);

        return HiveDriverUtil.setSchema(connection, hive3CDPSourceDTO.getUrl(), hive3CDPSourceDTO.getSchema());
    }

    /**
     * 在一个session 中开启事务
     * @param con
     */
    private void openSessionDbTxnManager(Connection con) {
        try (Statement conStatement = con.createStatement()) {
            //开启并发支持
            conStatement.execute("set hive.support.concurrency=true");
            //开启事务
            conStatement.execute("set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        } catch (Exception e) {
            // 不处理
            log.warn("open session Txn error,message:{}", e.getMessage(), e);
        }
    }

    /**
     * 处理 Hive 的 Properties 属性
     *
     * @param properties
     */
    private void dealProperties (Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        // 特殊处理 hive 的 key，增加 hiveconf: 属性 key 前缀
        Set<String> keys = properties.stringPropertyNames();
        for (String key : keys) {
            properties.put(HIVE_CONF_PREFIX + key, properties.getProperty(key));
            properties.remove(key);
        }
    }
}
