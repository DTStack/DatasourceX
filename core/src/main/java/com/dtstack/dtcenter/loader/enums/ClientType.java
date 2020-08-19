package com.dtstack.dtcenter.loader.enums;

import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.hdfs.HdfsFileClientCache;
import com.dtstack.dtcenter.loader.client.mq.KafkaClientCache;
import com.dtstack.dtcenter.loader.client.sql.DataSourceClientCache;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:00 2020/1/13
 * @Description：客户端类型
 */
public enum ClientType {

    /**
     * 数据源测试连通性相关，大部分使用这个
     */
    DATA_SOURCE_CLIENT(1, DataSourceClientCache.getInstance()),

    /**
     * KAFKA 的一些操作
     */
    KAFKA_CLIENT(2, KafkaClientCache.getInstance()),

    /**
     * HDFS 文件的一些操作，具体见
     */
    HDFS_CLIENT(3, HdfsFileClientCache.getInstance())
    ;

    private Integer clientType;

    private AbsClientCache clientCache;

    ClientType(Integer clientType, AbsClientCache clientCache) {
        this.clientType = clientType;
        this.clientCache = clientCache;
    }

    public Integer getClientType() {
        return clientType;
    }

    public AbsClientCache getClientCache() {
        return clientCache;
    }
}
