package com.dtstack.dtcenter.common.loader.tdbs.hdfs.client;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.tdbs.hdfs.HdfsConnFactory;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:53 2020/2/27
 * @Description：hdfs 客户端
 */
public class HdfsClient<T> extends AbsNoSqlClient<T> {
    private HdfsConnFactory hdfsConnFactory = new HdfsConnFactory();

    @Override
    public Boolean testCon(ISourceDTO source) {
        return hdfsConnFactory.testConn(source);
    }
}