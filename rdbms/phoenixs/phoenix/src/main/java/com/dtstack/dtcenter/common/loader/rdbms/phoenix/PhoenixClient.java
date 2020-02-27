package com.dtstack.dtcenter.common.loader.rdbms.phoenix;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:09 2020/2/27
 * @Description：Phoenix 客户端
 */
public class PhoenixClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new PhoenixConnFactory();
    }
}
