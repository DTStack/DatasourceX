package com.dtstack.dtcenter.common.loader.spark;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.common.utils.DBUtil;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SparkSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Spark 连接池工厂
 */
@Slf4j
public class SparkConnFactory extends ConnFactory {
    public SparkConnFactory() {
        this.driverName = DataBaseType.Spark.getDriverClassName();
        this.errorPattern = new SparkErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
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
                        throw new DtLoaderException("getHiveConnection error : " + e.getMessage(), e);
                    }
                }
        );

        return SparkThriftDriverUtil.setSchema(connection, sparkSourceDTO.getUrl(), sparkSourceDTO.getSchema());
    }
}
