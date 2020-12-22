package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：wangchuan
 * @Date ：Created in 下午2:08 2020/6/2
 * @Description：kafka客户端接口
 */
public interface IKafka<T> {

    /**
     * 校验 连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    Boolean testCon(ISourceDTO source);

    /**
     * 获取所有 Brokers 的地址
     *
     * @param source
     * @return
     * @throws Exception
     */
    String getAllBrokersAddress(ISourceDTO source);

    /**
     * 获取 所有 Topic 信息
     *
     * @param source
     * @return
     * @throws Exception
     */
    List<String> getTopicList(ISourceDTO source);

    /**
     * 创建 Topic
     *
     * @param source
     * @param kafkaTopic
     * @return
     * @throws Exception
     */
    Boolean createTopic(ISourceDTO source, KafkaTopicDTO kafkaTopic);

    /**
     * 获取特定 Topic 分区信息
     *
     * @param source
     * @param topic
     * @return
     * @throws Exception
     */
    List<T> getAllPartitions(ISourceDTO source, String topic);

    /**
     * 获取特定 Topic 所有分区的偏移量
     *
     * @param source
     * @param topic
     * @return
     * @throws Exception
     */
    List<KafkaOffsetDTO> getOffset(ISourceDTO source, String topic);

    /**
     * 获取预览数据
     * @param source
     * @param queryDTO
     * @deprecated since 1.4.0 in favor of
     * {@link #getPreview(ISourceDTO source, SqlQueryDTO queryDTO, String prevMode)}
     * @return
     * @throws Exception
     */
    @Deprecated
    List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取预览数据
     * @param source 数据源信息
     * @param queryDTO 查询条件
     * @param prevMode 预览模式
     * @return
     * @throws Exception
     */
    List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO, String prevMode);
}
