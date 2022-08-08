/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.loader.cache.client;

import com.alibaba.fastjson.serializer.DateCodec;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.dtstack.dtcenter.loader.classloader.DtClassLoader;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * client classloader 缓存
 *
 * @author ：wangchuan
 * date：Created in 上午9:43 2021/12/20
 * company: www.dtstack.com
 */
@Slf4j
public class ClassLoaderCache {

    /**
     * 存储 插件名称 - ClassLoader 键值对信息
     */
    private static final Map<String, ClassLoader> PLUGIN_CLASSLOADER = Maps.newConcurrentMap();

    /**
     * 获取类加载器
     *
     * @param pluginName classloader 名称
     * @return 当前插件对应的classloader
     * @throws Exception 插件加载时的异常信息
     */
    public static ClassLoader getClassLoader(String pluginName) throws Exception {
        ClassLoader classLoader = PLUGIN_CLASSLOADER.get(pluginName);
        if (null != classLoader) {
            return classLoader;
        }

        synchronized (ClassLoaderCache.class) {
            classLoader = PLUGIN_CLASSLOADER.get(pluginName);
            if (classLoader == null) {
                classLoader = getClassLoad(pluginName, getFileByPluginName(pluginName));
                PLUGIN_CLASSLOADER.put(pluginName, classLoader);
            }
        }

        dealFastJSON(pluginName, classLoader);
        return classLoader;
    }

    /**
     * 根据 jar 文件生成对应的 ClassLoader
     *
     * @param file jar 文件
     * @return 对应的 classloader
     * @throws MalformedURLException 读取异常信息
     */
    private static URLClassLoader getClassLoad(String pluginName, @NotNull File file) throws MalformedURLException {
        File[] files = file.listFiles();
        List<URL> urlList = new ArrayList<>();
        if (files.length == 0) {
            throw new DtLoaderException("The plugin folder setting is abnormal, please handle it again");
        }

        for (File f : files) {
            String jarName = f.getName();
            if (f.isFile() && jarName.endsWith(".jar")) {
                log.info("Data source plugin pulls Jar package, plugin name: {}, Jar package name: {}", pluginName, jarName);
                urlList.add(f.toURI().toURL());
            }
        }

        return new DtClassLoader(urlList.toArray(new URL[0]), Thread.currentThread().getContextClassLoader());
    }

    /**
     * 特殊处理序列化逻辑
     *
     * @param pluginName  插件包名称
     * @param classLoader 对应的 classloader
     */
    private static void dealFastJSON(String pluginName, ClassLoader classLoader) {
        // 处理 oracle 时间类型字段
        if (DataSourceType.Oracle.getPluginName().equals(pluginName)) {
            try {
                Class<?> loadClass = classLoader.loadClass("oracle.sql.DATE");
                SerializeConfig.getGlobalInstance().put(loadClass, DateCodec.instance);
                loadClass = classLoader.loadClass("oracle.sql.TIMESTAMP");
                SerializeConfig.getGlobalInstance().put(loadClass, DateCodec.instance);
            } catch (ClassNotFoundException e) {
                log.error("FastJSON Serialization tool exception", e);
            }
        }
    }

    /**
     * 根据插件名称获取文件
     *
     * @param pluginName 根据插件包名称获取对应的文件信息
     * @return jar file
     * @throws Exception 异常信息
     */
    @NotNull
    private static File getFileByPluginName(String pluginName) throws Exception {
        String plugin = String.format("%s/%s", RealClientCache.getUserDir(), pluginName).replaceAll("//*", "/");
        File finPut = new File(plugin);
        if (!finPut.exists()) {
            throw new Exception(String.format("%s directory not found", plugin));
        }
        return finPut;
    }
}
