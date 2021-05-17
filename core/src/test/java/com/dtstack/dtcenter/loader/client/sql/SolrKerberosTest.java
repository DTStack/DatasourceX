package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IKafka;
import com.dtstack.dtcenter.loader.client.IKerberos;
import com.dtstack.dtcenter.loader.dto.KafkaOffsetDTO;
import com.dtstack.dtcenter.loader.dto.KafkaTopicDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KafkaSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.SolrSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.kerberos.HadoopConfTool;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.common.requests.MetadataResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 11:27 2020/9/10
 * @Description：Kafka Kerberos 认证
 */
@Slf4j
public class SolrKerberosTest {


    private static final IClient client = ClientCache.getClient(DataSourceType.SOLR.getVal());

    SolrSourceDTO source = SolrSourceDTO.builder()
            .zkHost("worker:2181/solr")
            .build();

    @Before
    public void setUp() throws Exception {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put(HadoopConfTool.PRINCIPAL, "solr/worker@DTSTACK.COM");
        kerberosConfig.put(HadoopConfTool.PRINCIPAL_FILE, "/solr.keytab");
        kerberosConfig.put(HadoopConfTool.KEY_JAVA_SECURITY_KRB5_CONF, "/krb5.conf");
        source.setKerberosConfig(kerberosConfig);

        String localKerberosPath = SolrKerberosTest.class.getResource("/solr").getPath();
        IKerberos kerberos = ClientCache.getKerberos(DataSourceType.SOLR.getVal());
        kerberos.prepareKerberosForConnect(kerberosConfig, localKerberosPath);
    }

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void getAllDb () {
        List<String> databases = client.getAllDatabases(source, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    @Test
    public void getPreview() {
        List viewList = client.getPreview(source, SqlQueryDTO.builder().tableName("qianyi_test").previewNum(5).build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(viewList));
    }

    @Test
    public void getColumnMetaData() {
        List metaData = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("qianyi_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

}
