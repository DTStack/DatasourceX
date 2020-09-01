package com.dtstack.dtcenter.loader;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午7:42 2020/6/2
 * @Description：
 */
public interface IDownloader {

    boolean configure() throws Exception;

    List<String> getMetaInfo() throws Exception;

    Object readNext() throws Exception;

    boolean reachedEnd() throws Exception;

    boolean close() throws Exception;

    String getFileName();

    List<String> getContainers() throws Exception;
}
