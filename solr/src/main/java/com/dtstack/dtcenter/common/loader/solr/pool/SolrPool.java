package com.dtstack.dtcenter.common.loader.solr.pool;

import com.dtstack.dtcenter.common.loader.common.Pool;
import org.apache.solr.client.solrj.impl.CloudSolrClient;

/**
 * @company: www.dtstack.com
 * @Author ：qianyi
 * @Date ：Created in 下午5:14 2021/5/7
 * @Description：
 */
public class SolrPool extends Pool<CloudSolrClient> {

    private SolrPoolConfig config;

    public SolrPool(SolrPoolConfig config) {
        super(config, new SolrPoolFactory(config));
        this.config = config;
    }

    public SolrPoolConfig getConfig() {
        return config;
    }

    public void setConfig(SolrPoolConfig config) {
        this.config = config;
    }

}
