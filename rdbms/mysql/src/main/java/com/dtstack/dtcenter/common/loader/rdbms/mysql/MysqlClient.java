package com.dtstack.dtcenter.common.loader.rdbms.mysql;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.common.enums.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:18 2020/1/3
 * @Description：Mysql 客户端
 */
public class MysqlClient extends AbsRdbmsClient {
    public MysqlClient() {
        this.dbType = DataBaseType.MySql.getTypeName();
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new MysqlConnFactory();
    }
}
