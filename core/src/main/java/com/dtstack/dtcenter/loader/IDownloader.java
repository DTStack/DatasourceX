package com.dtstack.dtcenter.loader;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午7:42 2020/6/2
 * @Description：
 */
public interface IDownloader {

    void configure() throws Exception;

    List<String> getMetaInfo() throws Exception;

    Object readNext() throws Exception;

    boolean reachedEnd() throws Exception;

    void close() throws Exception;

    String getFileName();

}
