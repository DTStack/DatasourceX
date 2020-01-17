package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.common.enums.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:01 2020/1/6
 * @Description：Oracle 连接工厂
 */
public class OracleConnFactory extends ConnFactory {
    public OracleConnFactory() {
        driverName = DataBaseType.Oracle.getDriverClassName();
    }
}
