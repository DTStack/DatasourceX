package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.ITable;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.InceptorSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
public class InceptorTableTest {

    /**
     * 构造 inceptor 客户端
     */
    private static final IClient INCEPTOR_CLIENT = ClientCache.getClient(DataSourceType.INCEPTOR.getVal());

    /**
     * 构造 inceptor table 客户端
     */
    private static final ITable INCEPTOR_Table = ClientCache.getTable(DataSourceType.INCEPTOR.getVal());

    /**
     * 构建数据源信息
     */
    private static final InceptorSourceDTO INCEPTOR_SOURCE_DTO = InceptorSourceDTO.builder()
            .url("jdbc:hive2://tdh6-node3:10000/default;principal=hive/tdh6-node3@TDH")
            .schema("default")
            .defaultFS("hdfs://nameservice1")
            .config("{\n" +
                    "    \"dfs.ha.namenodes.nameservice1\": \"nn1,nn2\",\n" +
                    "    \"dfs.namenode.rpc-address.nameservice1.nn1\": \"172.16.100.192:8020\",\n" +
                    "    \"dfs.namenode.rpc-address.nameservice1.nn2\": \"172.16.101.204:8020\",\n" +
                    "    \"dfs.client.failover.proxy.provider.nameservice1\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\",\n" +
                    "    \"dfs.nameservices\": \"nameservice1\"\n" +
                    "}")
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        // 准备 Kerberos 参数
        Map<String, Object> kerberosConfig = new HashMap<>();
        String localKerberosPath = HiveKerberosTest.class.getResource("/tdh/inceptor").getPath();
        kerberosConfig.put("principal", "hive/tdh6-node3@TDH");
        kerberosConfig.put("principalFile", localKerberosPath + "/inceptor.keytab");
        kerberosConfig.put("java.security.krb5.conf", localKerberosPath + "/krb5.conf");
        INCEPTOR_SOURCE_DTO.setKerberosConfig(kerberosConfig);

        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_part").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_part (id int comment 'ID', name string comment '姓名_name') partitioned by (pt1 string,pt2 string, pt3 string)").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("alter table loader_test_part add partition (pt1 = 'a1', pt2 = 'b1', pt3 = 'c1') ").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("alter table loader_test_part add partition (pt1 = 'a2', pt2 = 'b2', pt3 = 'c2') ").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("alter table loader_test_part add partition  (pt1 = 'a3', pt2 = 'b3', pt3 = 'c3')").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_2").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_2 (id int comment 'ID', name string comment '姓名_name')").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test_3").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test_3 (id int comment 'ID', name string comment '姓名_name')").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("drop view if exists loader_test_5").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create view loader_test_5 as select * from loader_test_3").build();
        INCEPTOR_CLIENT.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, queryDTO);
    }

    /**
     * 获取所有分区
     */
    @Test
    public void showPartitions () {
        List<String> result = INCEPTOR_Table.showPartitions(INCEPTOR_SOURCE_DTO, "loader_test_part");
        System.out.println(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 删除表
     */
    @Test
    public void dropTable () {
        Boolean check = INCEPTOR_Table.dropTable(INCEPTOR_SOURCE_DTO, "loader_test_2");
        Assert.assertTrue(check);
    }

    /**
     * 重命名表
     */
    @Test
    public void renameTable () {
        INCEPTOR_Table.executeSqlWithoutResultSet(INCEPTOR_SOURCE_DTO, "drop table if exists loader_test_4");
        Boolean renameCheck = INCEPTOR_Table.renameTable(INCEPTOR_SOURCE_DTO, "loader_test_3", "loader_test_4");
        Assert.assertTrue(renameCheck);
    }

    /**
     * 修改表参数
     */
    @Test
    public void alterTableParams () {
        Map<String, String> params = Maps.newHashMap();
        params.put("comment", "test");
        Boolean alterCheck = INCEPTOR_Table.alterTableParams(INCEPTOR_SOURCE_DTO, "loader_test_part", params);
        Assert.assertTrue(alterCheck);
    }

    /**
     * 判断表是否是视图 - 是
     */
    @Test
    public void tableIsView () {
        Boolean check = INCEPTOR_Table.isView(INCEPTOR_SOURCE_DTO, null, "loader_test_5");
        Assert.assertTrue(check);
    }

    /**
     * 判断表是否是视图 - 否
     */
    @Test
    public void tableIsNotView () {
        Boolean check = INCEPTOR_Table.isView(INCEPTOR_SOURCE_DTO, null, "loader_test_5");
        Assert.assertTrue(check);
    }

}
