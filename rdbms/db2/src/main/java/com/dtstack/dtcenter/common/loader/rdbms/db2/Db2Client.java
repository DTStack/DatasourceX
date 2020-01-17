package com.dtstack.dtcenter.common.loader.rdbms.db2;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:09 2020/1/7
 * @Description：Db2 客户端
 */
public class Db2Client extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new Db2ConnFactory();
    }
}
