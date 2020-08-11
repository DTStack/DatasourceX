package com.dtstack.dtcenter.loader.enums;

import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.hdfs.mq.HdfsFileClientCache;
import com.dtstack.dtcenter.loader.client.mq.KafkaClientCache;
import com.dtstack.dtcenter.loader.client.sql.DataSourceClientCache;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:00 2020/1/13
 * @Description：客户端类型
 */
public enum ClientType {

    DATA_SOURCE_CLIENT(1, DataSourceClientCache.getInstance()),
    KAFKA_CLIENT(2, KafkaClientCache.getInstance()),
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
