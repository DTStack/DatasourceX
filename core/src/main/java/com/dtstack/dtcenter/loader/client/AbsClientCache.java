package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.DtClassLoader;
import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.utils.MD5Util;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:18 2020/1/13
 * 如果需要开启插件校验，请使用 startCheckFile 来校验文件
 * @Description：抽象客户端缓存
 */
@Slf4j
public abstract class AbsClientCache {
    protected static String userDir = String.format("%s/pluginLibs/", System.getProperty("user.dir"));

    /**
     * 是否校验插件改变
     */
    protected static AtomicBoolean checkFile = new AtomicBoolean(Boolean.FALSE);

    /**
     * 存储 插件名称 - 插件文件 MD5 信息
     */
    protected Map<String, String> pluginMd5 = Maps.newConcurrentMap();

    /**
     * 根据文件流生成当前的 ClassLoader
     *
     * @param file
     * @return
     * @throws MalformedURLException
     */
    protected URLClassLoader getClassLoad(String pluginName, @NotNull File file) throws MalformedURLException {
        File[] files = file.listFiles();
        List<URL> urlList = new ArrayList<>();
        if (files.length == 0) {
            throw new DtLoaderException("插件文件夹设置异常，请二次处理");
        }

        StringBuilder md5Builder = new StringBuilder();
        for (File f : files) {
            String jarName = f.getName();
            if (f.isFile() && jarName.endsWith(".jar")) {
                urlList.add(f.toURI().toURL());
                md5Builder.append(MD5Util.getMD5String(f));
            }
        }

        pluginMd5.put(pluginName, md5Builder.toString());
        return new DtClassLoader(urlList.toArray(new URL[urlList.size()]), this.getClass().getClassLoader());
    }

    /**
     * 获取 ClassLoader MD5 字符串
     *
     * @param file
     * @return
     */
    @Nullable
    protected static String getClassLoaderMd5(@NotNull File file) {
        File[] files = file.listFiles();
        List<URL> urlList = new ArrayList<>();
        if (files.length == 0) {
            return null;
        }

        StringBuilder md5Builder = new StringBuilder();
        for (File f : files) {
            String jarName = f.getName();
            if (f.isFile() && jarName.endsWith(".jar")) {
                try {
                    urlList.add(f.toURI().toURL());
                } catch (MalformedURLException e) {
                    log.error(e.getMessage(), e);
                    return null;
                }

                md5Builder.append(MD5Util.getMD5String(f));
            }
        }
        return md5Builder.toString();
    }

    /**
     * 开启插件文件校验
     */
    public static void startCheckFile() {
        checkFile.set(Boolean.TRUE);
    }
    
    /**
     * 修改插件包文件夹路径
     * @param dir
     */
    public static void setUserDir (String dir) {
        AbsClientCache.userDir = dir;
    }

    /**
     * 根据特定类型获取对应的客户端
     *
     * @param sourceName
     * @return
     * @throws ClientAccessException
     */
    public abstract IClient getClient(String sourceName) throws ClientAccessException;

    /**
     * 获取 kafka 对应的客户端
     *
     * @param sourceName
     * @return
     * @throws ClientAccessException
     */
    public abstract IKafka getKafka(String sourceName) throws ClientAccessException;

    /**
     * 获取 HDFS 对应的客户端
     *
     * @param sourceName
     * @return
     * @throws ClientAccessException
     */
    public abstract IHdfsFile getHdfs(String sourceName) throws ClientAccessException;
}
