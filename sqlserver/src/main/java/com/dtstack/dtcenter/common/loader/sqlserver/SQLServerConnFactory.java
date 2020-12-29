package com.dtstack.dtcenter.common.loader.sqlserver;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import lombok.extern.slf4j.Slf4j;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:30 2020/1/7
 * @Description：连接器工厂类
 */
@Slf4j
public class SQLServerConnFactory extends ConnFactory {
    public SQLServerConnFactory() {
        // 兼容 JTDS 逻辑
        try {
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error(e.getMessage(), e);
        }
        driverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        testSql = DataBaseType.SQLServer.getTestSql();
    }
}
