package com.dtstack.dtcenter.common.loader.mongo;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:24 2020/2/5
 * @Description：MongoDB 客户端
 */
@Slf4j
public class MongoDBClient<T> implements IClient<T> {
    @Override
    public Boolean testCon(ISourceDTO iSource) {
        return MongoDBUtils.checkConnection(iSource);
    }

    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        return MongoDBUtils.getTableList(iSource);
    }

    /********************************* 非关系型数据库无需实现的方法 ******************************************/
    @Override
    public Connection getCon(ISourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }
}