package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.SourceDTO;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:27 2020/1/13
 * @Description：客户端接口
 */
public interface IClient {
    /**
     * 获取 连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    Connection getCon(SourceDTO source) throws Exception;

    /**
     * 校验 连接
     *
     * @param source
     * @return
     */
    Boolean testCon(SourceDTO source) throws Exception;

    /**
     * 执行查询
     *
     * @param conn
     * @param sql
     * @return
     * @throws Exception
     */
    List<Map<String, Object>> executeQuery(Connection conn, String sql) throws Exception;

    /**
     * 执行查询
     *
     * @param source
     * @param sql
     * @return
     * @throws Exception
     */
    List<Map<String, Object>> executeQuery(SourceDTO source, String sql) throws Exception;
}
