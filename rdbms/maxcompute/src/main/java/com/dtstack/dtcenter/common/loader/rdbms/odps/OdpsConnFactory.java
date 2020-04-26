package com.dtstack.dtcenter.common.loader.rdbms.odps;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:48 2020/1/7
 * @Description：ODPS 连接工厂
 */
public class OdpsConnFactory extends ConnFactory {
    public OdpsConnFactory() {
        this.driverName = DataBaseType.MaxCompute.getDriverClassName();
    }

    @Override
    public Connection getConn(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
