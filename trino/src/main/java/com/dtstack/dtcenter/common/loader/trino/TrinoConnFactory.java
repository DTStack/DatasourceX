package com.dtstack.dtcenter.common.loader.trino;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * trino 数据源连接工厂类
 *
 * @author ：wangchuan
 * date：Created in 下午2:21 2021/9/9
 * company: www.dtstack.com
 */
public class TrinoConnFactory extends ConnFactory {

    public TrinoConnFactory() {
        driverName = DataBaseType.TRINO.getDriverClassName();
        this.errorPattern = new TrinoErrorPattern();
        testSql = DataBaseType.TRINO.getTestSql();
    }
}
