package com.dtstack.dtcenter.common.loader.rdbms.dm;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:23 2020/4/17
 * @Description：达梦连接工厂
 */
public class DmConnFactory extends ConnFactory {
    public DmConnFactory() {
        driverName = DataBaseType.DMDB.getDriverClassName();
    }
}
