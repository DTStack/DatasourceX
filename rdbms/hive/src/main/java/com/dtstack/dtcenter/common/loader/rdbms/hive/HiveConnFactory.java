package com.dtstack.dtcenter.common.loader.rdbms.hive;

import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.common.enums.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:07 2020/1/7
 * @Description：Hive 连接池工厂
 */
public class HiveConnFactory extends ConnFactory {
    public HiveConnFactory() {
        this.driverName = DataBaseType.HIVE.getDriverClassName();
    }
}
