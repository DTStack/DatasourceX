package com.dtstack.dtcenter.common.loader.rdbms.mysql;

import com.dtstack.dtcenter.loader.enums.DataBaseType;
import com.dtstack.dtcenter.loader.service.IRdbmsClient;
import com.dtstack.dtcenter.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.rdbms.common.RdbmsClient;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:18 2020/1/3
 * @Description：Mysql 客户端
 */
public class MysqlClient extends RdbmsClient implements IRdbmsClient {
    public MysqlClient() {
        this.dbType = DataBaseType.MySql.getTypeName();
    }

    @Override
    protected ConnFactory getConnFactory() {
        return new MysqlConnFactory();
    }
}
