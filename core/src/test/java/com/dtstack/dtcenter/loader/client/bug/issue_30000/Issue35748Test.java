package com.dtstack.dtcenter.loader.client.bug.issue_30000;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OracleSourceDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 10:58 2021/03/22
 * @Description：Oracle Blob & Clob 测试
 */
public class Issue35748Test {
    private static OracleSourceDTO source = OracleSourceDTO.builder()
            .url("jdbc:oracle:thin:@172.16.100.243:1521:orcl")
            .username("oracle")
            .password("oracle")
            .build();

    @BeforeClass
    public static void beforeClass() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table LOADER_TEST").build();
        try {
            client.executeSqlWithoutResultSet(source, queryDTO);
        } catch (Exception e){
            // oracle不支持 drop table if exists tableName 语法
        }
        queryDTO = SqlQueryDTO.builder().sql("create table LOADER_TEST (id int, clo clob, blo blob)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table LOADER_TEST is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into LOADER_TEST values (1, 'LOADER_TEST', null)").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

    }

    @Test
    public void test_for_issue() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.Oracle.getVal());
        List preview = client.getPreview(source, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        String previewJSON = JSONObject.toJSONString(preview);
        Assert.assertTrue(previewJSON.contains("LOADER_TEST"));
    }
}