package com.dtstack.dtcenter.common.loader.rdbms.odps;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:48 2020/1/7
 * @Description：ODPS 连接工厂
 */
public class OdpsConnFactory extends ConnFactory {
    public OdpsConnFactory() {
        this.driverName = DataBaseType.MaxCompute.getDriverClassName();
    }
}
