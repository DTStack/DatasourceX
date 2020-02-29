package com.dtstack.dtcenter.common.loader.rdbms.postgresql;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:53 2020/1/7
 * @Description：
 */
public class PostgresqlConnFactory extends ConnFactory {
    public PostgresqlConnFactory() {
        this.driverName = DataBaseType.PostgreSQL.getDriverClassName();
    }
}
