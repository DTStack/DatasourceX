package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.IHdfsWriter;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 23:58 2020/2/28
 * @Description：HDFS 测试
 */
public class HdfsTest {
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
    public void testCon() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HDFS.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void testHdfsWriter() throws Exception {
        IHdfsFile client = clientCache.getHdfs(DataSourceType.HDFS.getPluginName());
        IHdfsWriter writer = client.getHdfsWriter(FileFormat.PARQUET.getVal());
        HdfsWriterDTO writerDTO = JSONObject.parseObject("{\"columnsList\":[{\"key\":\"id\",\"part\":false,\"type\":\"int\"},{\"key\":\"name\",\"part\":false,\"type\":\"string\"}],\"fromFileName\":\"F:\\\\workspace\\\\java\\\\dtstack\\\\dt-center-ide\\\\upload\\\\c7a29b6f-a65a-4d0a-bddc-9373a5af1a9b\",\"fromLineDelimiter\":\",\",\"hdfsDirPath\":\"hdfs://ns1/user/hive/warehouse/dev.db/test_chener_0811\",\"keyList\":[{\"key\":\"id\"},{\"key\":\"name\"}],\"oriCharSet\":\"UTF-8\",\"startLine\":1,\"topLineIsTitle\":true}", HdfsWriterDTO.class);

        HdfsSourceDTO sourceDTO = new HdfsSourceDTO();
        sourceDTO.setDefaultFS("hdfs://ns1");
        sourceDTO.setConfig("{\"fs.defaultFS\":\"hdfs://ns1\",\"hadoop.proxyuser.admin.groups\":\"*\",\"javax.jdo.option.ConnectionDriverName\":\"com.mysql.jdbc.Driver\",\"dfs.replication\":\"2\",\"dfs.ha.fencing.methods\":\"sshfence\",\"dfs.client.failover.proxy.provider.ns1\":\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"typeName\":\"yarn2-hdfs2-hadoop2\",\"dfs.ha.fencing.ssh.private-key-files\":\"~/.ssh/id_rsa\",\"dfs.nameservices\":\"ns1\",\"javax.jdo.option.ConnectionURL\":\"jdbc:mysql://kudu2:3306/ide?useSSL=false\",\"dfs.safemode.threshold.pct\":\"0.5\",\"dfs.qjournal.write-txns.timeout.ms\":\"60000\",\"dfs.ha.namenodes.ns1\":\"nn1,nn2\",\"dfs.journalnode.rpc-address\":\"0.0.0.0:8485\",\"dfs.journalnode.http-address\":\"0.0.0.0:8480\",\"dfs.namenode.rpc-address.ns1.nn2\":\"kudu2:9000\",\"dfs.namenode.rpc-address.ns1.nn1\":\"kudu1:9000\",\"hive.metastore.warehouse.dir\":\"/user/hive/warehouse\",\"hive.server2.webui.host\":\"172.16.10.34\",\"dfs.namenode.shared.edits.dir\":\"qjournal://kudu1:8485;kudu2:8485;kudu3:8485/namenode-ha-data\",\"hive.metastore.schema.verification\":\"false\",\"javax.jdo.option.ConnectionUserName\":\"dtstack\",\"hive.server2.support.dynamic.service.discovery\":\"true\",\"javax.jdo.option.ConnectionPassword\":\"abc123\",\"hive.metastore.uris\":\"thrift://kudu1:9083\",\"hive.server2.thrift.port\":\"10000\",\"hive.exec.dynamic.partition.mode\":\"nonstrict\",\"version\":\"hadoop2\",\"ha.zookeeper.session-timeout.ms\":\"5000\",\"hadoop.tmp.dir\":\"/data/hadoop_${user.name}\",\"dfs.journalnode.edits.dir\":\"/data/dtstack/hadoop/journal\",\"hive.server2.zookeeper.namespace\":\"hiveserver2\",\"hive.server2.enable.doAs\":\"/false\",\"dfs.namenode.http-address.ns1.nn2\":\"kudu2:50070\",\"dfs.namenode.http-address.ns1.nn1\":\"kudu1:50070\",\"hadoop.proxyuser.admin.hosts\":\"*\",\"md5zip\":\"f22aab9f7107ced3d0291dabbe54e34a\",\"hive.exec.scratchdir\":\"/user/hive/warehouse\",\"hive.server2.webui.max.threads\":\"100\",\"hive.zookeeper.quorum\":\"kudu1:2181,kudu2:2181,kudu3:2181\",\"datanucleus.schema.autoCreateAll\":\"true\",\"hive.exec.dynamic.partition\":\"true\",\"hive.server2.thrift.bind.host\":\"kudu1\",\"ha.zookeeper.quorum\":\"kudu1:2181,kudu2:2181,kudu3:2181\",\"hive.server2.thrift.min.worker.threads\":\"200\",\"hive.server2.webui.port\":\"10002\",\"dfs.ha.automatic-failover.enabled\":\"true\"}");
        writer.writeByName(sourceDTO, writerDTO);
    }
}
