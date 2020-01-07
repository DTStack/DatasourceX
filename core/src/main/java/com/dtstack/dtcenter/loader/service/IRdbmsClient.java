package com.dtstack.dtcenter.loader.service;

import java.sql.Connection;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/1/3
 * @Description：客户端
 */
public interface IRdbmsClient {
    /**
     * 获取 连接
     *
     * @param url
     * @param prop
     * @return
     * @throws Exception
     */
    Connection getCon(String url, Properties prop) throws Exception;

    /**
     * 校验 连接
     *
     * @param url
     * @param prop
     * @return
     */
    Boolean testCon(String url, Properties prop);
}
