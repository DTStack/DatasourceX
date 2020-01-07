package com.dtstack.dtcenter.common.loader.rdbms.mysql;

import com.dtstack.dtcenter.loader.enums.DataBaseType;
import com.dtstack.dtcenter.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:11 2020/1/3
 * @Description：Mysql 连接
 */
public class MysqlConnFactory extends ConnFactory {
    public MysqlConnFactory() {
        driverName = DataBaseType.MySql.getDriverClassName();
        testSql = DataBaseType.MySql.getTestSql();
    }
}
