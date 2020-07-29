package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：连接器工厂类
 */
public class SQLServerConnFactory extends ConnFactory {
    public SQLServerConnFactory() {
        driverName = DataBaseType.SQLSSERVER_2017_LATER.getDriverClassName();
        testSql = DataBaseType.SQLSSERVER_2017_LATER.getTestSql();
    }
}
