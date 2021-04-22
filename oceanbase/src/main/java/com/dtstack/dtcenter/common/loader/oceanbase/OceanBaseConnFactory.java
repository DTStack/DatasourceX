package com.dtstack.dtcenter.common.loader.oceanbase;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：qianyi
 * @Date ：Created in 14:18 2021/4/21
 */
public class OceanBaseConnFactory extends ConnFactory {
    public OceanBaseConnFactory() {
        driverName = DataBaseType.OceanBase.getDriverClassName();
        this.errorPattern = new OceanBaseErrorPattern();
    }
}
