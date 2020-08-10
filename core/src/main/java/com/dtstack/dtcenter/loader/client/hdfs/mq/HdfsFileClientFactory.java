package com.dtstack.dtcenter.loader.client.hdfs.mq;

import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.google.common.collect.Maps;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:40 2020/8/10
 * @Description：Hdfs 文件操作客户端工厂
 */
public class HdfsFileClientFactory {

    /**
     * 存储 插件名称 - ClassLoader 键值对信息
     */
    private static Map<String, ClassLoader> pluginClassLoader = Maps.newConcurrentMap();

    /**
     * 去除特定的插件缓存
     *
     * @param pluginName
     */
    public static void removePlugin(String pluginName) {
        pluginClassLoader.remove(pluginName);
    }

    public static IHdfsFile createPluginClass(String pluginName) throws Exception {
        ClassLoader classLoader = pluginClassLoader.get(pluginName);
        return ClassLoaderCallBackMethod.callbackAndReset((ClassLoaderCallBack<IHdfsFile>) () -> {
            ServiceLoader<IHdfsFile> kafkas = ServiceLoader.load(IHdfsFile.class);
            Iterator<IHdfsFile> iClientIterator = kafkas.iterator();
            if (!iClientIterator.hasNext()) {
                throw new RuntimeException("暂不支持该插件类型: " + pluginName);
            }

            IHdfsFile hdfsFile = iClientIterator.next();
            return new HdfsFileProxy(hdfsFile);
        }, classLoader, true);
    }

    public static void addClassLoader(String pluginName, ClassLoader classLoader) {
        if (pluginClassLoader.containsKey(pluginName)) {
            return;
        }

        pluginClassLoader.put(pluginName, classLoader);
    }

    public static boolean checkContainClassLoader(String pluginName) {
        return pluginClassLoader.containsKey(pluginName);
    }
}
