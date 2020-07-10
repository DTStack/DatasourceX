package com.dtstack.dtcenter.common.loader.phoenix;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:32 2020/7/8
 * @Description：默认 Phoenix 连接工厂
 */
public class PhoenixConnFactory extends ConnFactory {
    public PhoenixConnFactory() {
        this.driverName = DataBaseType.Phoenix.getDriverClassName();
        this.testSql = DataBaseType.Phoenix.getTestSql();
    }
}
