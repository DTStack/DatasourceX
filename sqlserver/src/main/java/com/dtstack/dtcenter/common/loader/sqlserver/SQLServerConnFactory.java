package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：连接器工厂类
 */
public class SQLServerConnFactory extends ConnFactory {
    public SQLServerConnFactory() {
        driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        testSql = DataBaseType.SQLServer.getTestSql();
    }
}
