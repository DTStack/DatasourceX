package com.dtstack.dtcenter.common.loader.redis;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:38 2020/2/4
 * @Description：Redis 客户端
 */
public class RedisClient<T> implements IClient<T> {
    @Override
    public Boolean testCon(SourceDTO source) {
        return RedisUtils.checkConnection(source);
    }

    /********************************* 非关系型数据库无需实现的方法 ******************************************/
    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getFlinkColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getAllBrokersAddress(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTopicList(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean createTopic(SourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<T> getAllPartitions(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<List<Object>> getPreview(SourceDTO source, SqlQueryDTO queryDTO) {
        throw new DtLoaderException("Not Support");
    }
}
