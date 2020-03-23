package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.*;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:27 2020/1/13
 * @Description：客户端接口
 */
public interface IClient<T> {
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
     * @throws Exception
     */
    Boolean testCon(SourceDTO source);

    /**
     * 执行查询
     *
     * @param source
     * @param queryDTO 必填项 sql
     * @return
     * @throws Exception
     */
    List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 执行查询，无需结果集
     *
     * @param source
     * @param queryDTO 必填项 sql
     * @return
     * @throws Exception
     */
    Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 返回所有的表字段名称
     * 是否视图表，默认不过滤
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 返回字段 Java 类的标准名称
     * 字段名若不填则默认全部
     *
     * @param source
     * @param queryDTO 必填项 表名
     * @return
     * @throws Exception
     */
    List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 获取表字段属性
     * 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤
     *
     * @param source
     * @param queryDTO 必填项 表名
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 获取表注释
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /****************************************** Kafka 定制 ******************************************************/

    /**
     * 获取所有 Brokers 的地址
     *
     * @param source
     * @return
     * @throws Exception
     */
    String getAllBrokersAddress(SourceDTO source) throws Exception;

    /**
     * 获取 所有 Topic 信息
     *
     * @param source
     * @return
     * @throws Exception
     */
    List<String> getTopicList(SourceDTO source) throws Exception;

    /**
     * 创建 Topic
     *
     * @param source
     * @param kafkaTopic
     * @return
     * @throws Exception
     */
    Boolean createTopic(SourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception;

    /**
     * 获取特定 Topic 分区信息
     *
     * @param source
     * @param topic
     * @return
     * @throws Exception
     */
    List<T> getAllPartitions(SourceDTO source, String topic) throws Exception;

    /**
     * 获取特定 Topic 所有分区的偏移量
     *
     * @param source
     * @param topic
     * @return
     * @throws Exception
     */
    List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception;
}
