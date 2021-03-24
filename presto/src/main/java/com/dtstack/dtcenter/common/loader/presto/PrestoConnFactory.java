package com.dtstack.dtcenter.common.loader.presto;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * presto 数据源连接工厂类
 *
 * @author ：wangchuan
 * date：Created in 上午9:50 2021/3/23
 * company: www.dtstack.com
 */
public class PrestoConnFactory extends ConnFactory {

    public PrestoConnFactory() {
        driverName = DataBaseType.Presto.getDriverClassName();
        this.errorPattern = new PrestoErrorPattern();
        testSql = DataBaseType.Presto.getTestSql();
    }
}
