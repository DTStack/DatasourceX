package com.dtstack.dtcenter.common.loader.rdbms.postgresql;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:52 2020/1/7
 * @Description：Postgresql 客户端
 */
public class PostgresqlClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new PostgresqlCoonFactory();
    }
}
