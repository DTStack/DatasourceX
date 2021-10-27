package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.dto.source.TbdsKafkaSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class TbdsKafkaTest extends BaseTest {

    // 构建kafka数据源信息
    private static final TbdsKafkaSourceDTO source = TbdsKafkaSourceDTO.builder()
            .url("172.16.101.236:2181,172.16.101.17:2181,172.16.100.109:2181/kafka")
            .secureId("secureId")
            .secureKey("secureKey")
            .build();

    @BeforeClass
    public static void setUp() {}

    @Test
    public void testConForKafka() {
        IKafka client = ClientCache.getKafka(DataSourceType.TBDS_KAFKA.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }
}
