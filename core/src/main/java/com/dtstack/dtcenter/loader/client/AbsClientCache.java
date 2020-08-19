package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.exception.ClientAccessException;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

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
     * 修改插件包文件夹路径
     *
     * @param dir
     */
    public static void setUserDir(String dir) {
        AbsClientCache.userDir = dir;
    }

    /**
     * 获取插件包文件夹路径
     *
     * @return
     */
    public static String getUserDir() {
        return userDir;
    }

    /**
     * 根据特定类型获取对应的客户端
     *
     * @param sourceName
     * @return
     * @throws ClientAccessException
     */
    public IClient getClient(String sourceName) throws ClientAccessException {
        throw new DtLoaderException("请通过 DataSourceClientCache 获取其他数据库客户端");
    }

    /**
     * 获取 kafka 对应的客户端
     *
     * @param sourceName
     * @return
     * @throws ClientAccessException
     */
    public IKafka getKafka(String sourceName) throws ClientAccessException {
        throw new DtLoaderException("请通过 kafkaClientCache 获取 kafka 客户端");
    }

    /**
     * 获取 HDFS 对应的客户端
     *
     * @param sourceName
     * @return
     * @throws ClientAccessException
     */
    public IHdfsFile getHdfs(String sourceName) throws ClientAccessException {
        throw new DtLoaderException("请通过 HdfsClientCache 获取 Hdfs 文件客户端");
    }
}
