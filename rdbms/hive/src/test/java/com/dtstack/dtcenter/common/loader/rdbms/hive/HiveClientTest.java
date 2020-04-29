package com.dtstack.dtcenter.common.loader.rdbms.hive;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.apache.commons.lang3.BooleanUtils;
import org.junit.Test;

import java.util.List;

public class HiveClientTest {
    private static AbsRdbmsClient rdbsClient = new HiveClient();

    String krbStr = "{\"hive.exec.reducers.bytes.per.reducer\":\"67108864\",\"hive.optimize" +
            ".reducededuplication\":\"true\",\"hadoop.proxyuser.mapred.hosts\":\"*\",\"yarn.application" +
            ".classpath\":\"$HADOOP_CLIENT_CONF_DIR,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*," +
            "$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*\",\"dfs" +
            ".replication\":\"3\",\"hive.exec.copyfile.maxsize\":\"33554432\",\"hive.vectorized.execution" +
            ".enabled\":\"true\",\"hive.metastore.sasl.enabled\":\"true\",\"yarn.admin.acl\":\"*\",\"yarn.scheduler" +
            ".increment-allocation-vcores\":\"1\",\"hadoop.security.auth_to_local\":\"DEFAULT\",\"yarn.nodemanager" +
            ".remote-app-log-dir-suffix\":\"logs\",\"hive.limit.pushdown.memory.usage\":\"0.1\",\"yarn" +
            ".resourcemanager.webapp.address\":\"cdh3.cdhsite:8088\",\"hive.merge.mapredfiles\":\"false\",\"yarn" +
            ".scheduler.maximum-allocation-vcores\":\"8\",\"hive.map.aggr\":\"true\",\"spark.dynamicAllocation" +
            ".minExecutors\":\"1\",\"spark.yarn.executor.memoryOverhead\":\"244\",\"hadoop.proxyuser.HTTP" +
            ".hosts\":\"*\",\"hive.smbjoin.cache.rows\":\"10000\",\"hadoop.proxyuser.httpfs.hosts\":\"*\",\"dfs" +
            ".encrypt.data.transfer.cipher.suites\":\"AES/CTR/NoPadding\",\"hive.auto.convert.join\":\"true\",\"hive" +
            ".merge.mapfiles\":\"true\",\"hive.server2.authentication.kerberos.principal\":\"hive/_HOST@DTSTACK" +
            ".COM\",\"dfs.namenode.acls.enabled\":\"false\",\"yarn.scheduler.increment-allocation-mb\":\"512\",\"dfs" +
            ".encrypt.data.transfer.cipher.key.bitlength\":\"256\",\"yarn.resourcemanager.nm.liveness-monitor" +
            ".interval-ms\":\"1000\",\"hadoop.proxyuser.mapred.groups\":\"*\",\"hive.metastore.execute" +
            ".setugi\":\"true\",\"hadoop.security.group.mapping\":\"org.apache.hadoop.security" +
            ".ShellBasedUnixGroupsMapping\",\"net.topology.script.file.name\":\"/etc/hadoop/conf.cloudera" +
            ".yarn/topology.py\",\"hadoop.ssl.keystores.factory.class\":\"org.apache.hadoop.security.ssl" +
            ".FileBasedKeyStoresFactory\",\"hive.zookeeper.quorum\":\"cdh3.cdhsite,cdh2.cdhsite,cdh4.cdhsite\"," +
            "\"principalFile\":\"/Users/jialongyan/Desktop/nanqi011/yijing.keytab\",\"spark" +
            ".dynamicAllocation.enabled\":\"true\",\"hive.metastore.kerberos.principal\":\"hive/_HOST@DTSTACK.COM\"," +
            "\"yarn.resourcemanager.admin.address\":\"cdh3.cdhsite:8033\",\"yarn.resourcemanager.resource-tracker" +
            ".address\":\"cdh3.cdhsite:8031\",\"hive.merge.sparkfiles\":\"true\",\"dfs.domain.socket" +
            ".path\":\"/var/run/hdfs-sockets/dn\",\"dfs.datanode.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\"," +
            "\"hadoop.ssl.enabled\":\"false\",\"hadoop.proxyuser.hdfs.groups\":\"*\",\"dfs.namenode.name" +
            ".dir\":\"file:///data/dfs/nn\",\"dfs.client.use.datanode.hostname\":\"false\",\"hive.compute.query.using" +
            ".stats\":\"false\",\"dfs.namenode.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\",\"fs.trash" +
            ".interval\":\"1\",\"spark.dynamicAllocation.initialExecutors\":\"1\",\"yarn.scheduler" +
            ".minimum-allocation-vcores\":\"1\",\"dfs.client.domain.socket.data.traffic\":\"false\",\"yarn" +
            ".resourcemanager.admin.client.thread-count\":\"1\",\"dfs.block.access.token.enable\":\"true\",\"hadoop" +
            ".proxyuser.hive.hosts\":\"*\",\"spark.executor.memory\":\"1452802048\",\"yarn.resourcemanager.scheduler" +
            ".class\":\"org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler\",\"hive" +
            ".vectorized.groupby.checkinterval\":\"4096\",\"dfs.blocksize\":\"134217728\",\"hadoop.security" +
            ".instrumentation.requires.admin\":\"false\",\"hive.map.aggr.hash.percentmemory\":\"0.5\",\"io.file" +
            ".buffer.size\":\"65536\",\"yarn.resourcemanager.container.liveness-monitor.interval-ms\":\"600000\"," +
            "\"hadoop.proxyuser.oozie.hosts\":\"*\",\"hadoop.proxyuser.oozie.groups\":\"*\",\"yarn.resourcemanager" +
            ".scheduler.address\":\"cdh3.cdhsite:8030\",\"hadoop.proxyuser.yarn.hosts\":\"*\",\"hive.vectorized" +
            ".groupby.flush.percent\":\"0.1\",\"dfs.namenode.servicerpc-address\":\"cdh3.cdhsite:8022\",\"hive" +
            ".metastore.client.socket.timeout\":\"300\",\"hive.optimize.sort.dynamic.partition\":\"false\",\"hadoop" +
            ".ssl.require.client.cert\":\"false\",\"mapred.reduce.tasks\":\"-1\",\"yarn.resourcemanager" +
            ".address\":\"cdh3.cdhsite:8032\",\"dfs.client.read.shortcircuit.skip.checksum\":\"false\",\"dfs.namenode" +
            ".kerberos.principal.pattern\":\"*\",\"hive.vectorized.execution.reduce.enabled\":\"false\",\"yarn" +
            ".resourcemanager.resource-tracker.client.thread-count\":\"50\",\"hive.fetch.task" +
            ".conversion\":\"minimal\",\"hive.server2.authentication\":\"kerberos\",\"yarn.acl.enable\":\"true\"," +
            "\"hadoop.security.authorization\":\"true\",\"yarn.nm.liveness-monitor.expiry-interval-ms\":\"600000\"," +
            "\"hive.metastore.warehouse.dir\":\"/user/hive/warehouse\",\"yarn.am.liveness-monitor" +
            ".expiry-interval-ms\":\"600000\",\"hive.execution.engine\":\"mr\",\"hadoop.proxyuser.httpfs" +
            ".groups\":\"*\",\"spark.driver.memory\":\"966367641\",\"hive.cbo.enable\":\"false\",\"yarn" +
            ".resourcemanager.client.thread-count\":\"50\",\"spark.yarn.driver.memoryOverhead\":\"102\",\"hadoop" +
            ".security.authentication\":\"kerberos\",\"hadoop.proxyuser.hdfs.hosts\":\"*\",\"hive.optimize" +
            ".reducededuplication.min.reducer\":\"4\",\"dfs.client.use.legacy.blockreader\":\"false\",\"hadoop" +
            ".proxyuser.hue.hosts\":\"*\",\"hive.metastore.uris\":\"thrift://cdh3.cdhsite:9083\",\"hadoop.proxyuser" +
            ".yarn.groups\":\"*\",\"yarn.resourcemanager.am.max-attempts\":\"2\",\"yarn.resourcemanager" +
            ".max-completed-applications\":\"10000\",\"dfs.namenode.kerberos.internal.spnego" +
            ".principal\":\"HTTP/_HOST@DTSTACK.COM\",\"hadoop.proxyuser.hue.groups\":\"*\",\"io.compression" +
            ".codecs\":\"org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org" +
            ".apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.DeflateCodec,org.apache.hadoop.io" +
            ".compress.SnappyCodec,org.apache.hadoop.io.compress.Lz4Codec\",\"hadoop.proxyuser.hive.groups\":\"*\"," +
            "\"hive.cluster.delegation.token.store.class\":\"org.apache.hadoop.hive.thrift.MemoryTokenStore\",\"hive" +
            ".zookeeper.client.port\":\"2181\",\"fs.defaultFS\":\"hdfs://cdh3.cdhsite:8020\",\"hadoop.ssl.client" +
            ".conf\":\"ssl-client.xml\",\"yarn.resourcemanager.scheduler.client.thread-count\":\"50\",\"hadoop" +
            ".proxyuser.flume.hosts\":\"*\",\"hive.server2.logging.operation.log" +
            ".location\":\"/var/log/hive/operation_logs\",\"hive.merge.size.per.task\":\"268435456\",\"dfs.https" +
            ".port\":\"50470\",\"principal\":\"yijing@DTSTACK.COM\",\"dfs.encrypt.data.transfer.algorithm\":\"3des\"," +
            "\"dfs.client.read.shortcircuit\":\"false\",\"hive.merge.smallfiles.avgsize\":\"16777216\",\"dfs.namenode" +
            ".http-address\":\"cdh3.cdhsite:50070\",\"dfs.datanode.hdfs-blocks-metadata.enabled\":\"true\",\"hive" +
            ".exec.reducers.max\":\"1099\",\"hive.fetch.task.conversion.threshold\":\"268435456\",\"hadoop.ssl.server" +
            ".conf\":\"ssl-server.xml\",\"yarn.scheduler.minimum-allocation-mb\":\"1024\",\"yarn.resourcemanager" +
            ".principal\":\"yarn/_HOST@DTSTACK.COM\",\"hive.auto.convert.join.noconditionaltask.size\":\"20971520\"," +
            "\"hive.server2.logging.operation.enabled\":\"true\",\"dfs.https.address\":\"cdh3.cdhsite:50470\",\"hive" +
            ".support.concurrency\":\"true\",\"yarn.resourcemanager.amliveliness-monitor.interval-ms\":\"1000\"," +
            "\"yarn.nodemanager.remote-app-log-dir\":\"/tmp/logs\",\"spark.executor.cores\":\"4\",\"yarn.scheduler" +
            ".maximum-allocation-mb\":\"5616\",\"hive.warehouse.subdir.inherit.perms\":\"true\",\"spark" +
            ".dynamicAllocation.maxExecutors\":\"2147483647\",\"hive.optimize.bucketmapjoin.sortedmerge\":\"false\"," +
            "\"yarn.resourcemanager.webapp.https.address\":\"cdh3.cdhsite:8090\",\"hive.server2.enable" +
            ".doAs\":\"true\",\"hadoop.rpc.protection\":\"authentication\",\"fs.permissions.umask-mode\":\"022\"," +
            "\"spark.shuffle.service.enabled\":\"true\",\"dfs.nfs.kerberos.principal\":\"hdfs/_HOST@DTSTACK.COM\"," +
            "\"hadoop.proxyuser.flume.groups\":\"*\",\"hive.zookeeper.namespace\":\"hive_zookeeper_namespace_hive\"," +
            "\"hadoop.proxyuser.HTTP.groups\":\"*\"}";

    SourceDTO source = SourceDTO.builder()
            .url("jdbc:hive2://cdh3:10000/default;principal=hive/cdh3.cdhsite@DTSTACK.COM")
            .schema("default")
            .build();

    @Test
    public void getConnFactory() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("jdbc:hive2://cdh-impala2:10000/ceshis_pri")
                .username("root")
                .password("abc123")
                .build();
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("nanqi200228").filterPartitionColumns(false).build();
//        List<String> tableList = rdbsClient.getTableList(source, null);
        List<ColumnMetaDTO> columnMetaData = rdbsClient.getColumnMetaData(source, queryDTO);
        System.out.println(columnMetaData.size());
    }

    @Test
    public void testConnection() {
        Boolean isConnected = rdbsClient.testCon(source);
        assert BooleanUtils.isTrue(isConnected);
    }
}