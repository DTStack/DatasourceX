package com.dtstack.dtcenter.loader;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午7:42 2020/6/2
 * @Description：
 */
public interface IDownloader {

    /**
     * 配置下载器
     *
     * @return
     * @throws Exception
     */
    boolean configure() throws Exception;

    /**
     * 获取元数据信息
     *
     * @return
     * @throws Exception
     */
    List<String> getMetaInfo() throws Exception;

    /**
     * 读取下一行
     *
     * @return
     * @throws Exception
     */
    Object readNext() throws Exception;

    /**
     * 是否末行
     *
     * @return
     * @throws Exception
     */
    boolean reachedEnd() throws Exception;

    /**
     * 是否关闭
     *
     * @return
     * @throws Exception
     */
    boolean close() throws Exception;

    /**
     * 获取文件名
     *
     * @return
     */
    String getFileName();

}
