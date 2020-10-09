package com.dtstack.dtcenter.common.loader.libra;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:53 2020/2/29
 * @Description：Libra 连接工厂
 */
public class LibraConnFactory extends ConnFactory {
    public LibraConnFactory() {
        this.driverName = DataBaseType.PostgreSQL.getDriverClassName();
    }
}
