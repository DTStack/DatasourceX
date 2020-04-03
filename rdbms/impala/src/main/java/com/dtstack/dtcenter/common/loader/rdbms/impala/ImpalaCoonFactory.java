package com.dtstack.dtcenter.common.loader.rdbms.impala;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 工厂连接
 */
public class ImpalaCoonFactory extends ConnFactory {
    public ImpalaCoonFactory() {
        this.driverName = DataBaseType.Impala.getDriverClassName();
    }
}
