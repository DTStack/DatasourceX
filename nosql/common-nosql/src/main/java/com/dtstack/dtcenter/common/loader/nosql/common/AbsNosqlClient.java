package com.dtstack.dtcenter.common.loader.nosql.common;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:52 2020/1/17
 * @Description：非关系型数据库
 */
public abstract class AbsNosqlClient implements IClient {


    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name() + " GetConnection!");
    }

    @Override
    public abstract Boolean testCon(SourceDTO source) throws Exception;

    @Override
    public List<Map<String, Object>> executeQuery(Connection conn, String sql) throws Exception {
        throw new DtLoaderException("Not Support Connection Query!");
    }

    @Override
    public abstract List<Map<String, Object>> executeQuery(SourceDTO source, String sql) throws Exception;
}
