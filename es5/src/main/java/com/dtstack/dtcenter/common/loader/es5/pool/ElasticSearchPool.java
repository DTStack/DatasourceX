package com.dtstack.dtcenter.common.loader.es5.pool;

import com.dtstack.dtcenter.common.loader.common.Pool;
import org.elasticsearch.client.transport.TransportClient;

/**
 * es5 连接池
 *
 * @author ：wangchuan
 * date：Created in 下午3:04 2021/12/8
 * company: www.dtstack.com
 */
public class ElasticSearchPool extends Pool<TransportClient> {

    private ElasticSearchPoolConfig config;

    public ElasticSearchPool(ElasticSearchPoolConfig config){
        super(config, new ElasticSearchPoolFactory(config));
        this.config = config;
    }

    public ElasticSearchPoolConfig getConfig() {
        return config;
    }

    public void setConfig(ElasticSearchPoolConfig config) {
        this.config = config;
    }

}
