package com.dtstack.dtcenter.loader.client.mq;

import com.dtstack.dtcenter.loader.ClassLoaderCallBackMethod;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:23 2020/6/2
 * @Description：kafka代理
 */
@Slf4j
public class KafkaClientProxy<T> implements IKafka<T> {

    private IKafka targetClient;

    public KafkaClientProxy(IKafka targetClient) {
        this.targetClient = targetClient;
    }

    @Override
    public Boolean testCon(ISourceDTO source) {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.testCon(source),
                    targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    @Override
    public String getAllBrokersAddress(ISourceDTO source) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllBrokersAddress(source),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<String> getTopicList(ISourceDTO source) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getTopicList(source),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public Boolean createTopic(ISourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.createTopic(source, kafkaTopic),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<T> getAllPartitions(ISourceDTO source, String topic) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getAllPartitions(source, topic),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(ISourceDTO source, String topic) throws Exception {
        return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getOffset(source, topic),
                targetClient.getClass().getClassLoader(), true);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO)  {
        try {
            return ClassLoaderCallBackMethod.callbackAndReset(() -> targetClient.getPreview(source, queryDTO),
                    targetClient.getClass().getClassLoader(), true);
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
