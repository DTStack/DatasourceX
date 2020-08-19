package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:40 2020/1/6
 * @Description：关系型数据库插件客户端
 */
@Slf4j
@NoArgsConstructor
public class DataSourceClientCache extends AbsClientCache {
    private Map<String, IClient> defaultClientMap = Maps.newConcurrentMap();

    private static DataSourceClientCache singleton = new DataSourceClientCache();

    public static DataSourceClientCache getInstance() {
        return singleton;
    }

    /**
     * 根据数据源类型获取对应的客户端
     *
     * @param pluginName
     * @return
     */
    @Override
    public IClient getClient(String pluginName) throws ClientAccessException {
        try {
            IClient client = defaultClientMap.get(pluginName);
            if (client == null) {
                synchronized (defaultClientMap) {
                    client = defaultClientMap.get(pluginName);
                    if (client == null) {
                        client = DataSourceClientFactory.createPluginClass(pluginName);
                        defaultClientMap.put(pluginName, client);
                    }
                }
            }

            return client;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }
}
