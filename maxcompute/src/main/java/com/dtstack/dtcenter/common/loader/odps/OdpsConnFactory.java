package com.dtstack.dtcenter.common.loader.odps;

import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataBaseType;

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
    public Connection getConn(ISourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
