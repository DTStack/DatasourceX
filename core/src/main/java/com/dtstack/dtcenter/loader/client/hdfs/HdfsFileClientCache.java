package com.dtstack.dtcenter.loader.client.hdfs;

import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.google.common.collect.Maps;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:38 2020/8/10
 * @Description： HDFS 文件操作插件客户端
 */
@Slf4j
@NoArgsConstructor
public class HdfsFileClientCache extends AbsClientCache {

    private Map<String, IHdfsFile> defaultClientMap = Maps.newConcurrentMap();

    private static HdfsFileClientCache singleton = new HdfsFileClientCache();

    public static HdfsFileClientCache getInstance() {
        return singleton;
    }

    @Override
    public IHdfsFile getHdfs(String pluginName) throws ClientAccessException {
        try {
            IHdfsFile hdfsFile = defaultClientMap.get(pluginName);
            if (hdfsFile == null) {
                synchronized (defaultClientMap) {
                    hdfsFile = defaultClientMap.get(pluginName);
                    if (hdfsFile == null) {
                        hdfsFile = HdfsFileClientFactory.createPluginClass(pluginName);
                        defaultClientMap.put(pluginName, hdfsFile);
                    }
                }
            }

            return hdfsFile;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }
}
