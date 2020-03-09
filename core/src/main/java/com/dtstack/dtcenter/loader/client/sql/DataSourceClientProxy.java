package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.ClassLoaderCallBack;
import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.requests.MetadataResponse;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:19 2020/1/6
 * @Description 代理实现
 */
@Slf4j
public class DataSourceClientProxy implements IClient {
    private IClient targetClient;

    public DataSourceClientProxy(IClient targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getCon(source),
                    targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtCenterDefException(e.getMessage(), e);
        }
    }

    @Override
    public Boolean testCon(SourceDTO source) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.testCon(source),
                    targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeQuery(source, queryDTO),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {

        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.executeSqlWithoutResultSet(source,
                queryDTO), targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableList(source, queryDTO),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnClassInfo(source, queryDTO),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getColumnMetaData(source, queryDTO),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTableMetaComment(source, queryDTO),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public String getAllBrokersAddress(SourceDTO source) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllBrokersAddress(source),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<String> getTopicList(SourceDTO source) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTopicList(source),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public Boolean createTopic(SourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createTopic(source, kafkaTopic),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<MetadataResponse.PartitionMetadata> getAllPartitions(SourceDTO source, String topic) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllPartitions(source, topic),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getOffset(source, topic),
                targetClient.getClass().getClassLoader(), true);
    }
}
