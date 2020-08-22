package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 15:59 2020/8/19
 * @Description：HDFS 文件系统测试
 */
public class HdfsFileTest {
    private static final AbsClientCache clientCache = ClientType.HDFS_CLIENT.getClientCache();

    HdfsSourceDTO source = HdfsSourceDTO.builder()
            .defaultFS("hdfs://ns1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"kudu2:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha" +
                    ".ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"kudu1:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .build();

    @Test
    public void testHdfsWriter() throws Exception {
        HdfsWriterDTO writerDTO = JSONObject.parseObject("{\"columnsList\":[{\"key\":\"id\",\"part\":false,\"type\":\"int\"},{\"key\":\"name\",\"part\":false,\"type\":\"string\"}],\"fromFileName\":\"/Users/wangbin/Desktop/9c4e5c49-6af2-49f0-863e-293318a3e9a9\",\"fromLineDelimiter\":\",\",\"hdfsDirPath\":\"hdfs://ns1/user/hive/warehouse/dev.db/test_chener_0811\",\"fileFormat\":\"orc\",\"keyList\":[{\"key\":\"id\"},{\"key\":\"name\"}],\"oriCharSet\":\"UTF-8\",\"startLine\":1,\"topLineIsTitle\":true}", HdfsWriterDTO.class);
        IHdfsFile client = clientCache.getHdfs(DataSourceType.HDFS.getPluginName());
        int i = client.writeByName(source, writerDTO);
        System.out.println(i);
    }
}
