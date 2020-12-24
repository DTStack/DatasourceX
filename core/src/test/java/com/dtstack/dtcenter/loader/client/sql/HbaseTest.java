package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.client.AbsClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.comparator.RegexStringComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.PageFilter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.filter.SingleColumnValueFilter;
import com.dtstack.dtcenter.loader.dto.filter.TimestampFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.ClientType;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 23:36 2020/2/28
 * @Description：HBase 测试
 */
public class HbaseTest {
    private static final AbsClientCache clientCache = ClientType.DATA_SOURCE_CLIENT.getClientCache();

    HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.10.104,172.16.10.224,172.16.10.252:2181")
            .path("/hbase")
            .build();

    @Test
    public void testCon() throws Exception {
        Map map = JSONObject.parseObject("{\"hbase.server.thread.wakefrequency\":\"10000\",\"dfs.namenode.servicerpc-address.nameservice1.namenode59\":\"node2:8022\",\"hadoop.proxyuser.mapred.hosts\":\"*\",\"hbase.master.executor.closeregion.threads\":\"5\",\"dfs.replication\":\"3\",\"hbase.master.port\":\"60000\",\"hbase.master.executor.serverops.threads\":\"5\",\"hadoop.security.auth_to_local\":\"DEFAULT\",\"hbase.master.executor.openregion.threads\":\"5\",\"hbase.client.primaryCallTimeout.get\":\"10\",\"hbase.master.keytab.file\":\"hbase.keytab\",\"hadoop.proxyuser.HTTP.hosts\":\"*\",\"hbase.zookeeper.quorum\":\"node4,node3,node5\",\"hadoop.proxyuser.httpfs.hosts\":\"*\",\"dfs.encrypt.data.transfer.cipher.suites\":\"AES/CTR/NoPadding\",\"hbase.client.write.buffer\":\"2097152\",\"hbase.rpc.protection\":\"authentication\",\"hbase.rootdir\":\"hdfs://nameservice1/hbase\",\"dfs.namenode.acls.enabled\":\"true\",\"hbase.regionserver.kerberos.principal\":\"hbase/_HOST@DTSTACK.COM\",\"dfs.encrypt.data.transfer.cipher.key.bitlength\":\"256\",\"hbase.security.authentication\":\"kerberos\",\"hadoop.proxyuser.mapred.groups\":\"*\",\"hbase.splitlog.manager.timeout\":\"120000\",\"hadoop.security.group.mapping\":\"org.apache.hadoop.security.ShellBasedUnixGroupsMapping\",\"hadoop.ssl.keystores.factory.class\":\"org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory\",\"principalFile\":\"/Users/jialongyan/IdeaProjects/dtStack/dt-center-streamapp/kerberosConf/STREAM_0/USER_5/PROJECT_11/sparktest.keytab\",\"ha.zookeeper.quorum\":\"node3:2181,node4:2181,node5:2181\",\"hbase.snapshot.enabled\":\"true\",\"dfs.namenode.https-address.nameservice1.namenode59\":\"node2:50470\",\"hbase.row.level.authorization\":\"false\",\"dfs.domain.socket.path\":\"/var/run/hdfs-sockets/dn\",\"dfs.datanode.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\",\"zookeeper.session.timeout\":\"60000\",\"hadoop.proxyuser.hdfs.groups\":\"*\",\"dfs.client.use.datanode.hostname\":\"false\",\"dfs.namenode.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\",\"fs.trash.interval\":\"1\",\"hbase.regionserver.metahandler.count\":\"10\",\"dfs.client.domain.socket.data.traffic\":\"false\",\"hbase.master.kerberos.principal\":\"hbase/_HOST@DTSTACK.COM\",\"hbase.coprocessor.abortonerror\":\"false\",\"hadoop.proxyuser.hbase.hosts\":\"*\",\"dfs.block.access.token.enable\":\"true\",\"hadoop.proxyuser.hive.hosts\":\"*\",\"dfs.namenode.http-address.nameservice1.namenode41\":\"node1:50070\",\"dfs.blocksize\":\"134217728\",\"hadoop.security.instrumentation.requires.admin\":\"false\",\"dfs.namenode.https-address.nameservice1.namenode41\":\"node1:50470\",\"hadoop.proxyuser.oozie.hosts\":\"*\",\"hbase.ssl.enabled\":\"false\",\"hbase.master.logcleaner.ttl\":\"60000\",\"hbase.superuser\":\"sparktest\",\"hbase.master.ipc.address\":\"0.0.0.0\",\"hbase.regionserver.handler.count\":\"30\",\"hadoop.proxyuser.oozie.groups\":\"*\",\"dfs.ha.automatic-failover.enabled.nameservice1\":\"true\",\"hadoop.proxyuser.yarn.hosts\":\"*\",\"dfs.ha.namenodes.nameservice1\":\"namenode41,namenode59\",\"hbase.coprocessor.master.classes\":\"org.apache.hadoop.hbase.security.access.AccessController\",\"hadoop.ssl.require.client.cert\":\"false\",\"hbase.zookeeper.client.keytab.file\":\"hbase.keytab\",\"dfs.client.read.shortcircuit.skip.checksum\":\"false\",\"dfs.namenode.http-address.nameservice1.namenode59\":\"node2:50070\",\"dfs.nameservices\":\"nameservice1\",\"zookeeper.znode.rootserver\":\"root-region-server\",\"hadoop.security.authorization\":\"true\",\"hbase.client.retries.number\":\"35\",\"hadoop.proxyuser.httpfs.groups\":\"*\",\"hbase.regionserver.info.port\":\"60030\",\"dfs.client.hedged.read.threshold.millis\":\"500\",\"hadoop.security.authentication\":\"kerberos\",\"hbase.snapshot.master.timeout.millis\":\"60000\",\"hadoop.proxyuser.hdfs.hosts\":\"*\",\"dfs.client.use.legacy.blockreader\":\"false\",\"hadoop.proxyuser.hue.hosts\":\"*\",\"zookeeper.znode.parent\":\"/hbase\",\"hadoop.proxyuser.yarn.groups\":\"*\",\"hbase.master.info.port\":\"60010\",\"dfs.namenode.kerberos.internal.spnego.principal\":\"HTTP/_HOST@DTSTACK.COM\",\"hbase.cluster.distributed\":\"true\",\"hbase.snapshot.master.timeoutMillis\":\"60000\",\"hbase.master.handler.count\":\"25\",\"hbase.client.primaryCallTimeout.multiget\":\"10\",\"hbase.fs.tmp.dir\":\"/user/${user.name}/hbase-staging\",\"hbase.ipc.client.allowsInterrupt\":\"true\",\"hadoop.proxyuser.hue.groups\":\"*\",\"io.compression.codecs\":\"org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.Lz4Codec\",\"hbase.security.authorization\":\"true\",\"hadoop.proxyuser.hive.groups\":\"*\",\"fs.defaultFS\":\"hdfs://nameservice1\",\"hadoop.ssl.client.conf\":\"ssl-client.xml\",\"hadoop.proxyuser.flume.hosts\":\"*\",\"dfs.namenode.rpc-address.nameservice1.namenode59\":\"node2:8020\",\"hbase.client.pause\":\"100\",\"principal\":\"sparktest@DTSTACK.COM\",\"dfs.encrypt.data.transfer.algorithm\":\"3des\",\"dfs.client.read.shortcircuit\":\"false\",\"dfs.datanode.hdfs-blocks-metadata.enabled\":\"true\",\"hadoop.ssl.server.conf\":\"ssl-server.xml\",\"hbase.snapshot.region.timeout\":\"60000\",\"hadoop.proxyuser.hbase.groups\":\"*\",\"dfs.namenode.servicerpc-address.nameservice1.namenode41\":\"node1:8022\",\"dfs.client.hedged.read.threadpool.size\":\"0\",\"hbase.client.keyvalue.maxsize\":\"10485760\",\"dfs.client.failover.proxy.provider.nameservice1\":\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"hbase.client.scanner.caching\":\"100\",\"hbase.rpc.timeout\":\"60000\",\"hadoop.rpc.protection\":\"authentication\",\"dfs.namenode.rpc-address.nameservice1.namenode41\":\"node1:8020\",\"fs.permissions.umask-mode\":\"022\",\"hbase.zookeeper.property.clientPort\":\"2181\",\"hadoop.proxyuser.flume.groups\":\"*\",\"hadoop.proxyuser.HTTP.groups\":\"*\"}", Map.class);
        //source.setKerberosConfig(map);
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtCenterDefException("连接异常");
        }
    }

    @Test
    public void getTableList() throws Exception {
        Map map = JSONObject.parseObject("{\"hbase.server.thread.wakefrequency\":\"10000\",\"dfs.namenode.servicerpc-address.nameservice1.namenode59\":\"node2:8022\",\"hadoop.proxyuser.mapred.hosts\":\"*\",\"hbase.master.executor.closeregion.threads\":\"5\",\"dfs.replication\":\"3\",\"hbase.master.port\":\"60000\",\"hbase.master.executor.serverops.threads\":\"5\",\"hadoop.security.auth_to_local\":\"DEFAULT\",\"hbase.master.executor.openregion.threads\":\"5\",\"hbase.client.primaryCallTimeout.get\":\"10\",\"hbase.master.keytab.file\":\"hbase.keytab\",\"hadoop.proxyuser.HTTP.hosts\":\"*\",\"hbase.zookeeper.quorum\":\"node4,node3,node5\",\"hadoop.proxyuser.httpfs.hosts\":\"*\",\"dfs.encrypt.data.transfer.cipher.suites\":\"AES/CTR/NoPadding\",\"hbase.client.write.buffer\":\"2097152\",\"hbase.rpc.protection\":\"authentication\",\"hbase.rootdir\":\"hdfs://nameservice1/hbase\",\"dfs.namenode.acls.enabled\":\"true\",\"hbase.regionserver.kerberos.principal\":\"hbase/_HOST@DTSTACK.COM\",\"dfs.encrypt.data.transfer.cipher.key.bitlength\":\"256\",\"hbase.security.authentication\":\"kerberos\",\"hadoop.proxyuser.mapred.groups\":\"*\",\"hbase.splitlog.manager.timeout\":\"120000\",\"hadoop.security.group.mapping\":\"org.apache.hadoop.security.ShellBasedUnixGroupsMapping\",\"hadoop.ssl.keystores.factory.class\":\"org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory\",\"principalFile\":\"/Users/jialongyan/IdeaProjects/dtStack/dt-center-streamapp/kerberosConf/STREAM_0/USER_5/PROJECT_11/sparktest.keytab\",\"ha.zookeeper.quorum\":\"node3:2181,node4:2181,node5:2181\",\"hbase.snapshot.enabled\":\"true\",\"dfs.namenode.https-address.nameservice1.namenode59\":\"node2:50470\",\"hbase.row.level.authorization\":\"false\",\"dfs.domain.socket.path\":\"/var/run/hdfs-sockets/dn\",\"dfs.datanode.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\",\"zookeeper.session.timeout\":\"60000\",\"hadoop.proxyuser.hdfs.groups\":\"*\",\"dfs.client.use.datanode.hostname\":\"false\",\"dfs.namenode.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\",\"fs.trash.interval\":\"1\",\"hbase.regionserver.metahandler.count\":\"10\",\"dfs.client.domain.socket.data.traffic\":\"false\",\"hbase.master.kerberos.principal\":\"hbase/_HOST@DTSTACK.COM\",\"hbase.coprocessor.abortonerror\":\"false\",\"hadoop.proxyuser.hbase.hosts\":\"*\",\"dfs.block.access.token.enable\":\"true\",\"hadoop.proxyuser.hive.hosts\":\"*\",\"dfs.namenode.http-address.nameservice1.namenode41\":\"node1:50070\",\"dfs.blocksize\":\"134217728\",\"hadoop.security.instrumentation.requires.admin\":\"false\",\"dfs.namenode.https-address.nameservice1.namenode41\":\"node1:50470\",\"hadoop.proxyuser.oozie.hosts\":\"*\",\"hbase.ssl.enabled\":\"false\",\"hbase.master.logcleaner.ttl\":\"60000\",\"hbase.superuser\":\"sparktest\",\"hbase.master.ipc.address\":\"0.0.0.0\",\"hbase.regionserver.handler.count\":\"30\",\"hadoop.proxyuser.oozie.groups\":\"*\",\"dfs.ha.automatic-failover.enabled.nameservice1\":\"true\",\"hadoop.proxyuser.yarn.hosts\":\"*\",\"dfs.ha.namenodes.nameservice1\":\"namenode41,namenode59\",\"hbase.coprocessor.master.classes\":\"org.apache.hadoop.hbase.security.access.AccessController\",\"hadoop.ssl.require.client.cert\":\"false\",\"hbase.zookeeper.client.keytab.file\":\"hbase.keytab\",\"dfs.client.read.shortcircuit.skip.checksum\":\"false\",\"dfs.namenode.http-address.nameservice1.namenode59\":\"node2:50070\",\"dfs.nameservices\":\"nameservice1\",\"zookeeper.znode.rootserver\":\"root-region-server\",\"hadoop.security.authorization\":\"true\",\"hbase.client.retries.number\":\"35\",\"hadoop.proxyuser.httpfs.groups\":\"*\",\"hbase.regionserver.info.port\":\"60030\",\"dfs.client.hedged.read.threshold.millis\":\"500\",\"hadoop.security.authentication\":\"kerberos\",\"hbase.snapshot.master.timeout.millis\":\"60000\",\"hadoop.proxyuser.hdfs.hosts\":\"*\",\"dfs.client.use.legacy.blockreader\":\"false\",\"hadoop.proxyuser.hue.hosts\":\"*\",\"zookeeper.znode.parent\":\"/hbase\",\"hadoop.proxyuser.yarn.groups\":\"*\",\"hbase.master.info.port\":\"60010\",\"dfs.namenode.kerberos.internal.spnego.principal\":\"HTTP/_HOST@DTSTACK.COM\",\"hbase.cluster.distributed\":\"true\",\"hbase.snapshot.master.timeoutMillis\":\"60000\",\"hbase.master.handler.count\":\"25\",\"hbase.client.primaryCallTimeout.multiget\":\"10\",\"hbase.fs.tmp.dir\":\"/user/${user.name}/hbase-staging\",\"hbase.ipc.client.allowsInterrupt\":\"true\",\"hadoop.proxyuser.hue.groups\":\"*\",\"io.compression.codecs\":\"org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io.compress.SnappyCodec,org.apache.hadoop.io.compress.Lz4Codec\",\"hbase.security.authorization\":\"true\",\"hadoop.proxyuser.hive.groups\":\"*\",\"fs.defaultFS\":\"hdfs://nameservice1\",\"hadoop.ssl.client.conf\":\"ssl-client.xml\",\"hadoop.proxyuser.flume.hosts\":\"*\",\"dfs.namenode.rpc-address.nameservice1.namenode59\":\"node2:8020\",\"hbase.client.pause\":\"100\",\"principal\":\"sparktest@DTSTACK.COM\",\"dfs.encrypt.data.transfer.algorithm\":\"3des\",\"dfs.client.read.shortcircuit\":\"false\",\"dfs.datanode.hdfs-blocks-metadata.enabled\":\"true\",\"hadoop.ssl.server.conf\":\"ssl-server.xml\",\"hbase.snapshot.region.timeout\":\"60000\",\"hadoop.proxyuser.hbase.groups\":\"*\",\"dfs.namenode.servicerpc-address.nameservice1.namenode41\":\"node1:8022\",\"dfs.client.hedged.read.threadpool.size\":\"0\",\"hbase.client.keyvalue.maxsize\":\"10485760\",\"dfs.client.failover.proxy.provider.nameservice1\":\"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\"hbase.client.scanner.caching\":\"100\",\"hbase.rpc.timeout\":\"60000\",\"hadoop.rpc.protection\":\"authentication\",\"dfs.namenode.rpc-address.nameservice1.namenode41\":\"node1:8020\",\"fs.permissions.umask-mode\":\"022\",\"hbase.zookeeper.property.clientPort\":\"2181\",\"hadoop.proxyuser.flume.groups\":\"*\",\"hadoop.proxyuser.HTTP.groups\":\"*\"}", Map.class);
        //source.setKerberosConfig(map);
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        List<String> tableList = client.getTableList(source, null);
        System.out.println(tableList);
    }

    @Test
    public void getColumnMetaData() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        List dtstack = client.getColumnMetaData(source, SqlQueryDTO.builder().tableName("dtstack").build());
        System.out.println(dtstack);
    }

    @Test
    public void executorQuery() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        ArrayList<Filter> filters = new ArrayList<>();
        filters.add(pageFilter);
        String column = "info:sex";
        String column2 = "info:name";
        //String column2 = "liftInfo:girlFriend";
        ArrayList<String> columns = Lists.newArrayList(column,column2);
        SingleColumnValueFilter filter = new SingleColumnValueFilter("baseInfo".getBytes(), "age".getBytes(), CompareOp.EQUAL, new RegexStringComparator("."));
        //filter.setFilterIfMissing(true);
        //filters.add(filter);
        //filters.add(pageFilter);
        RowFilter rowFilter = new RowFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator("rowkey2".getBytes()));
        RowFilter rowFilter2 = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator("rowkey1".getBytes()));
        //filters.add(rowFilter);
        //filters.add(rowFilter2);
        List list = client.executeQuery(source, SqlQueryDTO.builder().tableName("yy_test").columns(columns).build());
        System.out.println(list);
    }

    @Test
    public void preview() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        List<List<Object>> result = client.getPreview(source, SqlQueryDTO.builder().tableName("yy_test").previewNum(10).build());
        System.out.println(result);
    }

    @Test
    public void timestampFilterTest_less_or_equal() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.LESS_OR_EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) <= 1602506453868L);
    }

    @Test
    public void timestampFilter_less() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.LESS, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) < 1602506453868L);
    }

    @Test
    public void timestampFilter_equal() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertEquals(1602506453868L, result.get(0).get("timestamp"));
    }

    @Test
    public void timestampFilterTest_granter() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.GREATER, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) > 1602506453868L);
    }

    @Test
    public void timestampFilterTest_granter_or_equal() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(1);
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) >= 1602506453868L);
    }

    @Test
    public void timestampAndRowKeyFilterTest_granter_or_equal() throws Exception {
        IClient client = clientCache.getClient(DataSourceType.HBASE.getPluginName());
        PageFilter pageFilter = new PageFilter(100);
        RowFilter rowFilter = new RowFilter(CompareOp.LESS, new BinaryComparator("auto9".getBytes()));
        TimestampFilter timestampFilter = new TimestampFilter(CompareOp.GREATER_OR_EQUAL, 1602506453868L);
        List<Filter> filters = Lists.newArrayList(timestampFilter, pageFilter, rowFilter);
        List<Map<String, Object>> result = client.executeQuery(source, SqlQueryDTO.builder().tableName("jmt_test_1602506450578").hbaseFilter(filters).build());
        Assert.assertTrue(((Long) result.get(0).get("timestamp")) >= 1602506453868L);
    }
}
