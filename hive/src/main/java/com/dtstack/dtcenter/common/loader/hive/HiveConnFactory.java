package com.dtstack.dtcenter.common.loader.hive;

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

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
@Slf4j
public class HiveConnFactory extends ConnFactory {
    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE.getDriverClassName();
        this.errorPattern = new HiveErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        HiveSourceDTO hiveSourceDTO = (HiveSourceDTO) iSource;

        Connection connection = KerberosLoginUtil.loginWithUGI(hiveSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        String urlWithoutSchema = HiveDriverUtil.removeSchema(hiveSourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, hiveSourceDTO.getUsername(),
                                hiveSourceDTO.getPassword());
                    } catch (Exception e) {
                        throw new DtLoaderException("getHiveConnection error : " + e.getMessage(), e);
                    }
                }
        );

        return HiveDriverUtil.setSchema(connection, hiveSourceDTO.getUrl(), hiveSourceDTO.getSchema());
    }
}
