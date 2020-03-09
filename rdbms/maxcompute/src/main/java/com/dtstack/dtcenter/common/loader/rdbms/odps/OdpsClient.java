package com.dtstack.dtcenter.common.loader.rdbms.odps;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:47 2020/1/7
 * @Description：ODPS 客户端
 */
public class OdpsClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new OdpsConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MAXCOMPUTE;
    }
}
