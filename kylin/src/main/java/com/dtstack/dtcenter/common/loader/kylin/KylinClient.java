package com.dtstack.dtcenter.common.loader.kylin;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:14 2020/1/7
 * @Description：Kylin 客户端
 */
public class KylinClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new KylinConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Kylin;
    }
}
