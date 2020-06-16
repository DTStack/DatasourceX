package com.dtstack.dtcenter.common.loader.clickhouse;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:58 2020/1/7
 * @Description：ClickHouse 连接工厂
 */
public class ClickhouseConnFactory extends ConnFactory {
    public ClickhouseConnFactory() {
        this.driverName = DataBaseType.Clickhouse.getDriverClassName();
    }
}
