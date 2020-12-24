package com.dtstack.dtcenter.common.loader.impala;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ImpalaSourceDTO;
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
        this.driverName = DataBaseType.Impala.getDriverClassName();
    }

    @Override
    public Connection getConn(ISourceDTO iSource) throws Exception {
        init();
        ImpalaSourceDTO impalaSourceDTO = (ImpalaSourceDTO) iSource;
        Connection connection = KerberosUtil.loginWithUGI(impalaSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<Connection>) () -> {
                    try {
                        return super.getConn(impalaSourceDTO);
                    } catch (Exception e) {
                        throw new DtCenterDefException("getImpalaConnection error : " + e.getMessage(), e);
                    }
                }
        );

        return ImpalaDriverUtil.setSchema(connection, impalaSourceDTO.getSchema());
    }
}
