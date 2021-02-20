package com.dtstack.dtcenter.common.loader.hive1;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.Hive1SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
@Slf4j
public class HiveConnFactory extends ConnFactory {
    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE1X.getDriverClassName();
        this.testSql = DataBaseType.HIVE1X.getTestSql();
        this.errorPattern = new Hive1ErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        Hive1SourceDTO hive1SourceDTO = (Hive1SourceDTO) iSource;

        Connection connection = KerberosLoginUtil.loginWithUGI(hive1SourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        String urlWithoutSchema = HiveDriverUtil.removeSchema(hive1SourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, hive1SourceDTO.getUsername(),
                                hive1SourceDTO.getPassword());
                    } catch (SQLException e) {
                        // 对异常进行统一处理
                        throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
                    }
                }
        );

        return HiveDriverUtil.setSchema(connection, hive1SourceDTO.getUrl(), hive1SourceDTO.getSchema());
    }
}
