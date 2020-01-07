package com.dtstack.dtcenter.loader.rdbms.client;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.DtClassLoader;
import com.dtstack.dtcenter.loader.enums.DataBaseType;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.dtstack.dtcenter.loader.service.IRdbmsClient;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:40 2020/1/6
 * @Description：关系型数据库插件客户端
 */
public class RdbmsClientCache {
    private static final Logger LOG = LoggerFactory.getLogger(RdbmsClientCache.class);

    private static String userDir = System.getProperty("user.dir");

    private Map<DataBaseType, IRdbmsClient> defaultClientMap = Maps.newConcurrentMap();

    private static RdbmsClientCache singleton = new RdbmsClientCache();

    private RdbmsClientCache() {
    }

    public static RdbmsClientCache getInstance() {
        return singleton;
    }

    /**
     * 根据数据源类型获取对应的客户端
     *
     * @param sourceTye
     * @return
     */
    public IRdbmsClient getClient(String sourceTye) throws ClientAccessException {
        try {
            DataBaseType dataBaseType = DataBaseType.valueOf(sourceTye);
            if (null == dataBaseType) {
                throw new DtCenterDefException("数据源类型不存在");
            }
            IRdbmsClient client = defaultClientMap.get(sourceTye);
            if (client == null) {
                synchronized (defaultClientMap) {
                    client = buildPluginClient(dataBaseType);
                    defaultClientMap.putIfAbsent(dataBaseType, client);
                }
            }

            return client;
        } catch (Throwable e) {
            throw new ClientAccessException(e);
        }
    }

    private IRdbmsClient buildPluginClient(DataBaseType dataBaseType) throws Exception {
        loadComputerPlugin(dataBaseType);
        return RdbmsClientFactory.createPluginClass(dataBaseType);
    }

    private void loadComputerPlugin(DataBaseType dataBaseType) throws Exception {
        if(RdbmsClientFactory.checkContainClassLoader(dataBaseType)){
            return;
        }

        String plugin = String.format("%s/pluginLibs/%s", userDir, dataBaseType.getTypeName());
        File finput = new File(plugin);
        if(!finput.exists()){
            throw new Exception(String.format("%s directory not found",plugin));
        }

        RdbmsClientFactory.addClassLoader(dataBaseType, getClassLoad(finput));
    }

    private URLClassLoader getClassLoad(File finput) throws MalformedURLException {
        File[] files = finput.listFiles();
        List<URL> urlList = new ArrayList<>();
        int index = 0;
        if (files!=null && files.length>0){
            for(File f : files){
                String jarName = f.getName();
                if(f.isFile() && jarName.endsWith(".jar")){
                    urlList.add(f.toURI().toURL());
                    index = index+1;
                }
            }
        }
        URL[] urls = urlList.toArray(new URL[urlList.size()]);
        return new DtClassLoader(urls, this.getClass().getClassLoader());
    }

}
