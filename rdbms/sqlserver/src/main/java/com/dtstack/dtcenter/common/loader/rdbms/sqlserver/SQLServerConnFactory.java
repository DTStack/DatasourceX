package com.dtstack.dtcenter.common.loader.rdbms.sqlserver;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：连接器工厂类
 */
public class SQLServerConnFactory extends ConnFactory {
    public SQLServerConnFactory() {
        driverName = DataBaseType.SQLServer.getDriverClassName();
        testSql = DataBaseType.SQLServer.getTestSql();
    }
}
