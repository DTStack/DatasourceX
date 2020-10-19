package com.dtstack.dtcenter.common.loader.gbase;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:58 2020/1/7
 * @Description：GBase8a 连接工厂
 */
public class GbaseConnFactory extends ConnFactory {
    public GbaseConnFactory() {
        this.driverName = DataBaseType.GBase8a.getDriverClassName();
    }
}
