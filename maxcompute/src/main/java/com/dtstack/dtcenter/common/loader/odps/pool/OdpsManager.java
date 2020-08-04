package com.dtstack.dtcenter.common.loader.odps.pool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.odps.common.OdpsFields;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.cache.pool.manager.Manager;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.OdpsSourceDTO;
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
public class OdpsManager implements Manager{

    private static Logger logger = LoggerFactory.getLogger(OdpsManager.class);

    private volatile static OdpsManager manager;

    private volatile Map<String, OdpsPool> sourcePool = Maps.newConcurrentMap();

    private volatile Map<String, ReentrantLock> lockPool = Maps.newConcurrentMap();

    private static final String ODPS_KEY = "endPoint:%s,accessId:%s,accessKey:%s,project:%s,packageAuthorizedProject:%s,accountType:%s";

    private OdpsManager() {}

    public static OdpsManager getInstance() {
        if (null == manager) {
            synchronized (OdpsManager.class) {
                if (null == manager) {
                    manager = new OdpsManager();
                }
            }
        }
        return manager;
    }

    @Override
    public OdpsPool getConnection(ISourceDTO source) {
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
        OdpsPool odpsPool = null;
        try {
            lock.lock();
            odpsPool = sourcePool.get(key);
            if (odpsPool == null) {
                odpsPool = initSource(source);
                sourcePool.putIfAbsent(key, odpsPool);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (lock.isLocked()) {
                lock.unlock();
            }
        }
        return odpsPool;
    }

    /**
     * 初始化odps pool
     * @param source
     * @return odps pool
     */
    @Override
    public OdpsPool initSource(ISourceDTO source) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) source;
        PoolConfig poolConfig = odpsSourceDTO.getPoolConfig();
        OdpsPoolConfig config = new OdpsPoolConfig();
        config.setMaxWaitMillis(poolConfig.getConnectionTimeout());
        config.setMinIdle(poolConfig.getMinimumIdle());
        config.setMaxIdle(poolConfig.getMaximumPoolSize());
        config.setMaxTotal(poolConfig.getMaximumPoolSize());
        config.setTimeBetweenEvictionRunsMillis(poolConfig.getTimeBetweenEvictionRunsMillis());
        config.setMinEvictableIdleTimeMillis(poolConfig.getMinEvictableIdleTimeMillis());
        // 闲置实例校验标识，如果校验失败会删除当前实例
        config.setTestWhileIdle(poolConfig.getTestWhileIdle());
        JSONObject odpsConfig = JSON.parseObject(odpsSourceDTO.getConfig());
        // 配置odps连接信息
        config.setOdpsServer(odpsConfig.getString(OdpsFields.KEY_ODPS_SERVER));
        config.setAccessId(odpsConfig.getString(OdpsFields.KEY_ACCESS_ID));
        config.setAccessKey(odpsConfig.getString(OdpsFields.KEY_ACCESS_KEY));
        config.setProject(odpsConfig.getString(OdpsFields.KEY_PROJECT));
        config.setPackageAuthorizedProject(odpsConfig.getString(OdpsFields.PACKAGE_AUTHORIZED_PROJECT));
        config.setAccountType(odpsConfig.getString(OdpsFields.KEY_ACCOUNT_TYPE));
        OdpsPool pool = new OdpsPool(config);
        //初始化 5 个实例
        pool.addObjects(poolConfig.getInitialSize());
        return pool;
    }

    /**
     * 获取odps唯一key
     * @param sourceDTO
     * @return
     */
    private String getPrimaryKey (ISourceDTO sourceDTO) {
        OdpsSourceDTO odpsSourceDTO = (OdpsSourceDTO) sourceDTO;
        JSONObject odpsConfig = JSON.parseObject(odpsSourceDTO.getConfig());
        return String.format(ODPS_KEY,
                odpsConfig.getString(OdpsFields.KEY_ODPS_SERVER),
                odpsConfig.getString(OdpsFields.KEY_ACCESS_ID),
                odpsConfig.getString(OdpsFields.KEY_ACCESS_KEY),
                odpsConfig.getString(OdpsFields.KEY_PROJECT),
                odpsConfig.getString(OdpsFields.PACKAGE_AUTHORIZED_PROJECT),
                odpsConfig.getString(OdpsFields.KEY_ACCOUNT_TYPE));
    }

}
