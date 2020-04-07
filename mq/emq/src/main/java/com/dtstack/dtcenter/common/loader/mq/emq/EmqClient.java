package com.dtstack.dtcenter.common.loader.mq.emq;

import com.dtstack.dtcenter.common.loader.kafkas.common.AbsMQClient;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.List;

/**
 * Date: 2020/4/7
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class EmqClient extends AbsMQClient {
    @Override
    public Boolean testCon(SourceDTO source) {
        return EMQUtils.checkConnection(source.getUrl(), source.getUsername(), source.getPassword());
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
    public List getAllPartitions(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
