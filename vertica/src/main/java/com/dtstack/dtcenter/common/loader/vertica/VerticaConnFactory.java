package com.dtstack.dtcenter.common.loader.vertica;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:03 2020/12/8
 * @Description：Vertica 工厂
 */
public class VerticaConnFactory  extends ConnFactory {
    public VerticaConnFactory() {
        driverName = "com.vertica.jdbc.Driver";
        testSql = "select 1";
    }
}
