package com.dtstack.dtcenter.common.loader.rdbms.greenplum;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:13 2020/4/10
 * @Description：Greenplum 工厂
 */
public class GreenplumFactory extends ConnFactory {
    public GreenplumFactory() {
        driverName = DataBaseType.Greenplum6.getDriverClassName();
    }
}
