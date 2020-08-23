package com.dtstack.dtcenter.common.loader.kylin;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 19:15 2020/1/7
 * @Description：Kylin 连接工厂
 */
public class KylinConnFactory extends ConnFactory {
    public KylinConnFactory() {
        this.driverName = DataBaseType.Kylin.getDriverClassName();
        this.testSql = DataBaseType.Kylin.getTestSql();
    }
}
