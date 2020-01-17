package com.dtstack.dtcenter.common.loader.rdbms.oracle;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 12:00 2020/1/6
 * @Description：Oracle 客户端
 */
public class OracleClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new OracleConnFactory();
    }
}
