package com.dtstack.dtcenter.common.loader.spark;

import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Spark 连接池工厂
 */
@Slf4j
public class SparkConnFactory extends ConnFactory {
    /**
     * HIVE 属性 Conn 设置语句
     */
    private static final String HIVE_CONF_SQL = "SET %s = %s";

    public SparkConnFactory() {
        this.driverName = DataBaseType.Spark.getDriverClassName();
        this.errorPattern = new SparkErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        init();
        SparkSourceDTO sparkSourceDTO = (SparkSourceDTO) iSource;

        Connection connection = KerberosLoginUtil.loginWithUGI(sparkSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        String urlWithoutSchema = SparkThriftDriverUtil.removeSchema(sparkSourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, sparkSourceDTO.getUsername(),
                                sparkSourceDTO.getPassword());
                    } catch (SQLException e) {
                        // 对异常进行统一处理
                        throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
                    }
                }
        );

        // 后置处理 SparkConnection 逻辑
        dealConnectionProperties(connection, DBUtil.stringToProperties(taskParams));

        return SparkThriftDriverUtil.setSchema(connection, sparkSourceDTO.getUrl(), sparkSourceDTO.getSchema());
    }

    /**
     * 处理 Hive 的 Properties 属性
     *
     * @param properties
     */
    private void dealConnectionProperties (Connection connection, Properties properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }

        // 特殊处理 hive 的 key，增加 hiveconf: 属性 key 前缀
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            if (entry.getKey() instanceof String && entry.getValue() instanceof String) {
                try {
                    DBUtil.executeSqlWithoutResultSet(connection, String.format(HIVE_CONF_SQL, entry.getKey(), entry.getValue()), false);
                } catch (Exception e) {
                    log.warn("deal SparkSql Connection properties error : {}", e.getMessage(), e);
                }
            }
        }
    }
}
