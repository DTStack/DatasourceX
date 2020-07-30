package com.dtstack.dtcenter.common.loader.db2;

import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:09 2020/1/7
 * @Description：Db2 工厂类
 */
public class Db2ConnFactory extends ConnFactory {
    public Db2ConnFactory() {
        this.driverName = DataBaseType.DB2.getDriverClassName();
    }
}
