package com.dtstack.dtcenter.common.loader.hive2;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
@Slf4j
public class HiveConnFactory extends ConnFactory {
    // Hive 属性前缀
    private static final String HIVE_CONF_PREFIX = "hiveconf:";

    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE.getDriverClassName();
        this.errorPattern = new HiveErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        init();
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;

        Connection connection = KerberosLoginUtil.loginWithUGI(hiveSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        Properties properties = DBUtil.stringToProperties(taskParams);
                        // 特殊处理 properties 属性
                        dealProperties(properties);
                        properties.put(DtClassConsistent.PublicConsistent.USER, hiveSourceDTO.getUsername() == null ? "" : hiveSourceDTO.getUsername());
                        properties.put(DtClassConsistent.PublicConsistent.PASSWORD, hiveSourceDTO.getPassword() == null ? "" : hiveSourceDTO.getPassword());
                        String urlWithoutSchema = HiveDriverUtil.removeSchema(hiveSourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, properties);
                    } catch (Exception e) {
                        // 对异常进行统一处理
                        throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
                    }
                }
        );

        return HiveDriverUtil.setSchema(connection, hiveSourceDTO.getUrl(), hiveSourceDTO.getSchema());
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
