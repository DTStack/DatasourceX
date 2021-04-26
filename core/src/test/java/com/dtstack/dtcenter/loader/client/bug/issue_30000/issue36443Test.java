package com.dtstack.dtcenter.loader.client.bug.issue_30000;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HiveSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 *
 * bug描述：查询 sql 类似于 'select id, null as name from xxx;' 调用 download 方法报错
 *
 * bug链接：http://zenpms.dtstack.cn/zentao/bug-view-36443.html
 *
 * bug解决：对查询字段 null 进行特殊处理
 *
 * @author ：wangchuan
 * date：Created in 下午3:02 2021/4/19
 * company: www.dtstack.com
 */
public class issue36443Test {

    /**
     * 构造hive客户端
     */
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.HIVE.getVal());

    /**
     * 查询字段
     */
    private static final List<String> QUERY_COLUMNS = Lists.newArrayList("id", "name", "null", "NULL");

    /**
     * 构建数据源信息
     */
    private static final HiveSourceDTO HIVE_SOURCE_DTO = HiveSourceDTO.builder()
            .url("jdbc:hive2://172.16.100.214:10000/default")
            .schema("default")
            .defaultFS("hdfs://ns1")
            .username("admin")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.ns1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn2\": \"172.16.101.227:9000\",\n" +
                    "    \"dfs.client.failover.proxy.provider.ns1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.namenode.rpc-address.ns1.nn1\": \"172.16.101.196:9000\",\n" +
                    "    \"dfs.nameservices\": \"ns1\"\n" +
                    "}")
            .poolConfig(PoolConfig.builder().build())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass()  {
        System.setProperty("HADOOP_USER_NAME", "admin");
        /*--------------------------------text-------------------------------------*/
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_36443_text").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_36443_text (id int, name string) stored as textfile").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_36443_text values (1, 'loader_test')").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        /*--------------------------------parquet----------------------------------*/
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_36443_parquet").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_36443_parquet (id int, name string) stored as parquet").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_36443_parquet values (1, 'loader_test')").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        /*--------------------------------orc-------------------------------------*/
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_36443_orc").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_36443_orc (id int, name string) stored as orc").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test_36443_orc values (1, 'loader_test')").build();
        CLIENT.executeSqlWithoutResultSet(HIVE_SOURCE_DTO, queryDTO);
    }

    @Test
    public void test_for_issue_text () throws Exception {
        IDownloader textDownload = CLIENT.getDownloader(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_36443_text").columns(QUERY_COLUMNS).build());
        while (!textDownload.reachedEnd()) {
            List<String> row = (List<String>) textDownload.readNext();
            Assert.assertEquals(4, row.size());
            Assert.assertNull(row.get(2));
            Assert.assertNull(row.get(3));
        }
    }

    @Test
    public void test_for_issue_parquet () throws Exception {
        IDownloader parquetDownload = CLIENT.getDownloader(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_36443_parquet").columns(QUERY_COLUMNS).build());
        while (!parquetDownload.reachedEnd()) {
            List<String> row = (List<String>) parquetDownload.readNext();
            Assert.assertEquals(4, row.size());
            Assert.assertNull(row.get(2));
            Assert.assertNull(row.get(3));
        }
    }

    @Test
    public void test_for_issue_orc () throws Exception {
        IDownloader orcDownload = CLIENT.getDownloader(HIVE_SOURCE_DTO, SqlQueryDTO.builder().tableName("loader_test_36443_orc").columns(QUERY_COLUMNS).build());
        while (!orcDownload.reachedEnd()) {
            List<String> row = (List<String>) orcDownload.readNext();
            Assert.assertEquals(4, row.size());
            Assert.assertNull(row.get(2));
            Assert.assertNull(row.get(3));
        }
    }

}
