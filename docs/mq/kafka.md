## kafka client

### 一、插件包名称

名称：**kafka**

### 二、对应数据源sourceDTO及参数说明

[KafkaSourceDTO](/core/src/main/java/com/dtstack/dtcenter/loader/dto/source/KafkaSourceDTO.java)

参数说明：

- **url**
    - 描述：kafka 使用zk 地址
    - 必选：否
    - 默认值：无


- **brokerUrls**
    - 描述：kafka broker地址
    - 必选：否
    - 默认值：无


- **kerberosConfig**
    - 描述：kerberos 配置信息
    - 必选：否
    - 默认值：无

#### 三、支持的方发及使用demo

##### IClient客户端使用

构造sourceDTO

```$java
        KafkaSourceDTO source = KafkaSourceDTO.builder()
                    .url("172.16.101.236:2181,172.16.101.17:2181,172.16.100.109:2181/kafka")
                    .build();
```

###### 1. 校验数据源连通性

入参类型：

- KafkaSourceDTO：数据源连接信息

出参类型：

- Boolean：连接信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

##### IKafka客户端使用

构造sourceDTO

```$java
        KafkaSourceDTO source = KafkaSourceDTO.builder()
                    .url("172.16.101.236:2181,172.16.101.17:2181,172.16.100.109:2181/kafka")
                    .build();
```

###### 1. 校验数据源连通性

入参类型：

- KafkaSourceDTO：数据源连接信息

出参类型：

- Boolean：连接信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        Boolean isConnected = client.testCon(sourceDTO);
```

###### 2. 获取所有broker地址

入参类型：

- KafkaSourceDTO：数据源连接信息

出参类型：

- String：broker地址

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        String brokersAddress = client.getAllBrokersAddress(source);
```

###### 3. 获取所有的 topic

入参类型：

- KafkaSourceDTO：数据源连接信息

出参类型：

- List<String>：topic 集合

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<String> topicList = client.getTopicList(source);
```

###### 4. 创建 kafka topic

入参类型：

- KafkaSourceDTO：数据源连接信息
- KafkaTopicDTO：topic 信息

出参类型：

- Boolean：创建结果

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        KafkaTopicDTO topicDTO = KafkaTopicDTO.builder()
                        .partitions(3)
                        .replicationFactor((short) 1)
                        .topicName("loader_test")
                        .build();
        Boolean clientTopic = client.createTopic(source, topicDTO);
```

###### 5. 获取 topic 偏移量

入参类型：

- KafkaSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：

- List<KafkaOffsetDTO>：偏移量信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaOffsetDTO> offset = client.getOffset(source, "topic_test");
```

###### 6. kafka 数据预览

入参类型：

- KafkaSourceDTO：数据源连接信息
- SqlQueryDTO：查询信息

出参类型：

- List<List<Object>>：预览数据信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("topic_test").build();
        List<List<Object>> results = client.getPreview(source, sqlQueryDTO, "latest");
```

###### 7. 获取 kafka topic partition 信息

入参类型：

- KafkaSourceDTO：数据源连接信息
- String：topic partition信息

出参类型：

- List<List<Object>>：预览数据信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaPartitionDTO> partitionDTOS = client.getTopicPartitions(source, "topic_test");
```

###### 8. 获取特定 Topic 分区信息

入参类型：

- KafkaSourceDTO：数据源连接信息
- String：topic 信息

出参类型：

- List<T>：分区信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List partitionDTOS = client.getAllPartitions(source, "topic_test");
```

###### 9. 从kafka 中消费数据

入参类型：

- KafkaSourceDTO：数据源连接信息
- String：topic 信息
- Integer：最大条数
- String：从哪里开始消费
- Long：消费启始位置
- Integer：最大等待时间，单位秒

出参类型：

- List<String>：kafka数据

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<String> data = client.consumeData(source, topic, collectNum, offsetReset, timestampOffset, maxTimeWait);
```

###### 10. 获取所有的消费者组

入参类型：

- KafkaSourceDTO：数据源连接信息

出参类型：

- List<String>：消费者组列表

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<String> data = client.listConsumerGroup(source);
```

###### 11. 获取指定topic下的所有的消费者组

入参类型：

- KafkaSourceDTO：数据源连接信息
- String：topic

出参类型：

- List<String>：消费者组列表

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<String> data = client.listConsumerGroupByTopic(source);
```

###### 12.获取 kafka 消费者组详细信息

入参类型：

- KafkaSourceDTO：数据源连接信息
- String：groupId 消费者组

出参类型：

- List<KafkaConsumerDTO>：消费者组详细信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> data = client.getGroupInfoByGroupId(source, groupId);
```

###### 13.获取 kafka 指定topic 下消费者组详细信息

入参类型：

- KafkaSourceDTO：数据源连接信息
- String：topic kafka主题

出参类型：

- List<KafkaConsumerDTO>：消费者组详细信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> data = client.getGroupInfoByTopic(source, topic);
```

###### 14.获取 kafka 指定topic下指定消费者组详细信息

入参类型：

- KafkaSourceDTO：数据源连接信息
- String： groupId 消费者组
- String：topic kafka主题

出参类型：

- List<KafkaConsumerDTO>：消费者组详细信息

使用：

```$java
        IKafka client = ClientCache.getKafka(DataSourceType.KAFKA.getVal());
        List<KafkaConsumerDTO> data = client.getGroupInfoByGroupIdAndTopic(source, groupId, topic);
```