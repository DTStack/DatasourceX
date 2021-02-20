package com.dtstack.dtcenter.common.loader.oracle;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:01 2020/1/6
 * @Description：Oracle 连接工厂
 */
public class OracleConnFactory extends ConnFactory {
    public OracleConnFactory() {
        driverName = DataBaseType.Oracle.getDriverClassName();
        errorPattern = new OracleErrorPattern();
    }
}
