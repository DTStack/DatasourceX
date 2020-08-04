package com.dtstack.dtcenter.common.loader.es.pool;

import com.dtstack.dtcenter.loader.cache.pool.manager.Manager;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午3:30 2020/8/3
 * @Description：
 */
public class ElasticSearchManager implements Manager{

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchManager.class);

    private volatile static ElasticSearchManager manager;

    private volatile Map<String, ElasticSearchPool> sourcePool = Maps.newConcurrentMap();

    private volatile Map<String, ReentrantLock> lockPool = Maps.newConcurrentMap();

    private static final String ES_KEY = "address:%s,username:%s,password:%s";

    private ElasticSearchManager() {}

    public static ElasticSearchManager getInstance() {
        if (null == manager) {
            synchronized (ElasticSearchManager.class) {
                if (null == manager) {
                    manager = new ElasticSearchManager();
                }
            }
        }
        return manager;
    }

    @Override
    public ElasticSearchPool getConnection(ISourceDTO source) {
        String key = getPrimaryKey(source).intern();
        ReentrantLock lock = lockPool.get(key);
        if (lock == null) {
            synchronized (key) {
                if (lock == null) {
                    lock = new ReentrantLock();
                    lockPool.put(key, lock);
                }
            }
        }
        ElasticSearchPool elasticSearchPool = null;
        try {
            lock.lock();
            elasticSearchPool = sourcePool.get(key);
            if (elasticSearchPool == null) {
                elasticSearchPool = initSource(source);
                sourcePool.putIfAbsent(key, elasticSearchPool);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (lock.isLocked()) {
                lock.unlock();
            }
        }
        return elasticSearchPool;
    }

    /**
     * 初始化es pool
     * @param source
     * @return
     */
    @Override
    public ElasticSearchPool initSource(ISourceDTO source) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) source;
        PoolConfig poolConfig = esSourceDTO.getPoolConfig();
        ElasticSearchPoolConfig config = new ElasticSearchPoolConfig();
        config.setMaxWaitMillis(poolConfig.getConnectionTimeout());
        config.setMinIdle(poolConfig.getMinimumIdle());
        config.setMaxIdle(poolConfig.getMaximumPoolSize());
        config.setMaxTotal(poolConfig.getMaximumPoolSize());
        config.setTimeBetweenEvictionRunsMillis(poolConfig.getTimeBetweenEvictionRunsMillis());
        config.setMinEvictableIdleTimeMillis(poolConfig.getMinEvictableIdleTimeMillis());
        // 闲置实例校验标识，如果校验失败会删除当前实例
        config.setTestWhileIdle(poolConfig.getTestWhileIdle());

        config.setUsername(esSourceDTO.getUsername());
        config.setPassword(esSourceDTO.getPassword());
        config.setNodes(new HashSet<>(Arrays.asList(esSourceDTO.getUrl().split(","))));

        ElasticSearchPool pool = new ElasticSearchPool(config);
        pool.addObjects(poolConfig.getInitialSize());
        return pool;
    }

    private String getPrimaryKey(ISourceDTO sourceDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) sourceDTO;
        return String.format(ES_KEY, esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
    }
}
