package com.dtstack.dtcenter.common.loader.nosql.common;

import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.*;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    protected static final Logger LOG = LoggerFactory.getLogger(AbsNosqlClient.class);

    @Override
    public abstract Boolean testCon(SourceDTO source);

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
    public List<MetadataResponse.PartitionMetadata> getAllPartitions(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
