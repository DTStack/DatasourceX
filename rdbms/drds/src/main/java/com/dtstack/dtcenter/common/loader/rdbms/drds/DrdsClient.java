package com.dtstack.dtcenter.common.loader.rdbms.drds;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

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
}
