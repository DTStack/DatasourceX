package com.dtstack.dtcenter.common.loader.drds;

import com.dtstack.dtcenter.common.loader.rdbms.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataSourceType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:01 2020/1/7
 * @Description：Drds 客户端
 */
public class DrdsClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new DrdsConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.MySQL;
    }
}
