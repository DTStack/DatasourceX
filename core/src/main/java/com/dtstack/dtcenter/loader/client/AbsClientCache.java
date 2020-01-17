package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.DtClassLoader;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:18 2020/1/13
 * @Description：抽象客户端缓存
 */
public abstract class AbsClientCache {
    protected static String userDir = System.getProperty("user.dir");

    protected URLClassLoader getClassLoad(File finput) throws MalformedURLException {
        File[] files = finput.listFiles();
        List<URL> urlList = new ArrayList<>();
        int index = 0;
        if (files != null && files.length > 0) {
            for (File f : files) {
                String jarName = f.getName();
                if (f.isFile() && jarName.endsWith(".jar")) {
                    urlList.add(f.toURI().toURL());
                    index = index + 1;
                }
            }
        }
        URL[] urls = urlList.toArray(new URL[urlList.size()]);
        return new DtClassLoader(urls, this.getClass().getClassLoader());
    }

    /**
     * 根据特定类型获取对应的客户端
     *
     * @param typeName
     * @return
     * @throws ClientAccessException
     */
    public abstract IClient getClient(String typeName) throws ClientAccessException;
}
