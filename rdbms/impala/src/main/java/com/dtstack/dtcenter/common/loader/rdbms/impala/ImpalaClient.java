package com.dtstack.dtcenter.common.loader.rdbms.impala;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:16 2020/1/7
 * @Description：Impala 连接
 */
public class ImpalaClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new ImpalaCoonFactory();
    }
}
