package com.dtstack.dtcenter.common.loader.drds;

import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:01 2020/1/7
 * @Description：Drds 连接工厂
 */
public class DrdsConnFactory extends ConnFactory {
    public DrdsConnFactory() {
        this.driverName = DataBaseType.DRDS.getDriverClassName();
    }
}