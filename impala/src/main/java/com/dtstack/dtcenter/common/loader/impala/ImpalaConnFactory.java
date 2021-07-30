package com.dtstack.dtcenter.common.loader.impala;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

import java.security.PrivilegedAction;
import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 工厂连接
 */
@Slf4j
public class ImpalaConnFactory extends ConnFactory {
    public ImpalaConnFactory() {
        this.driverName = "com.cloudera.impala.jdbc.Driver";
        this.errorPattern = new ImpalaErrorPattern();
    }

    @Override
    public Connection getConn(ISourceDTO iSource, String taskParams) throws Exception {
        init();
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) iSource;
        Connection connection = KerberosLoginUtil.loginWithUGI(impalaSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        return super.getConn(impalaSourceDTO, taskParams);
                    } catch (Exception e) {
                        throw new DtLoaderException(e.getMessage(), e);
                    }
                }
        );

        return ImpalaDriverUtil.setSchema(connection, impalaSourceDTO.getSchema());
    }
}
