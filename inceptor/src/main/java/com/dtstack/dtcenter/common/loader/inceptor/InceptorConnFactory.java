package com.dtstack.dtcenter.common.loader.inceptor;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.InceptorSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * inceptor 连接工厂
 *
 * @author ：wangchuan
 * date：Created in 下午2:19 2021/5/6
 * company: www.dtstack.com
 */
@Slf4j
public class InceptorConnFactory extends ConnFactory {

    public InceptorConnFactory() {
        this.driverName = DataBaseType.INCEPTOR.getDriverClassName();
        this.errorPattern = new InceptorErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        init();
        InceptorSourceDTO inceptorSourceDTO = (InceptorSourceDTO) iSource;
        Connection connection = KerberosLoginUtil.loginWithUGI(inceptorSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        DriverManager.setLoginTimeout(30);
                        String urlWithoutSchema = InceptorDriverUtil.removeSchema(inceptorSourceDTO.getUrl());
                        return DriverManager.getConnection(urlWithoutSchema, inceptorSourceDTO.getUsername(),
                                inceptorSourceDTO.getPassword());
                    } catch (SQLException e) {
                        // 对异常进行统一处理
                        throw new DtLoaderException(errorAdapter.connAdapter(e.getMessage(), errorPattern), e);
                    }
                }
        );
        return InceptorDriverUtil.setSchema(connection, inceptorSourceDTO.getUrl(), inceptorSourceDTO.getSchema());
    }
}
