package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.rdbms.common.RdbmsClient;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:00 2020/1/6
 * @Description：Oracle 客户端
 */
public class OracleClient extends RdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }
}
