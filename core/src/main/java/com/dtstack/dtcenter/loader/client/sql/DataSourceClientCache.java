package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:40 2020/1/6
 * @Description：关系型数据库插件客户端
 */
public class DataSourceClientCache extends AbsClientCache {
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceClientCache.class);

    private Map<DataSourceType, IClient> defaultClientMap = Maps.newConcurrentMap();

    private static DataSourceClientCache singleton = new DataSourceClientCache();

    private DataSourceClientCache() {
    }

    public static DataSourceClientCache getInstance() {
        return singleton;
    }

    /**
     * 根据数据源类型获取对应的客户端
     *
     * @param sourceName
     * @return
     */
    @Override
    public IClient getClient(String sourceName) throws ClientAccessException {
        try {
            DataSourceType sourceType = DataSourceType.valueOf(sourceName);
            if (null == sourceType) {
                throw new DtCenterDefException("数据源类型不存在");
            }
            IClient client = defaultClientMap.get(sourceType);
            if (client == null) {
                synchronized (defaultClientMap) {
                    client = buildPluginClient(sourceType);
                    defaultClientMap.putIfAbsent(sourceType, client);
                }
            }

            return client;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    private IClient buildPluginClient(DataSourceType sourceType) throws Exception {
        loadComputerPlugin(sourceType);
        return DataSourceClientFactory.createPluginClass(sourceType);
    }

    private void loadComputerPlugin(DataSourceType sourceType) throws Exception {
        if (DataSourceClientFactory.checkContainClassLoader(sourceType)) {
            return;
        }

        String plugin = String.format("%s/pluginLibs/%s", userDir, DataSourceType.getBaseType(sourceType).getTypeName());
        File finput = new File(plugin);
        if (!finput.exists()) {
            throw new Exception(String.format("%s directory not found", plugin));
        }

        DataSourceClientFactory.addClassLoader(sourceType, getClassLoad(finput));
    }
}
