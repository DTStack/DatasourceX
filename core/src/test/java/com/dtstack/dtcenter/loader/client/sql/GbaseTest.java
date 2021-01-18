package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.GBaseSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 11:06 2020/4/16
 * @Description：gbase 8a 测试
 */
public class GbaseTest {
    private static GBaseSourceDTO source = GBaseSourceDTO.builder()
            .url("jdbc:gbase://172.16.100.186:5258/dev_db")
            .username("dev")
            .password("dev123")
            .build();

    @BeforeClass
    public static void beforeClass() {
        IClient client = ClientCache.getClient(DataSourceType.GBase_8a.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test (id int COMMENT 'ID', name varchar(50) COMMENT '名称') comment 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test values (1, 'loader_test')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getConnFactory() {
        IClient client = ClientCache.getClient(DataSourceType.GBase_8a.getVal());
        Boolean isConnected = client.testCon(source);
        if (!isConnected) {
            throw new DtLoaderException("数据源连接异常");
        }
    }

    @Test
    public void getTableList() {
        IClient client = ClientCache.getClient(DataSourceType.GBase_8a.getVal());
        List tableList = client.getTableList(source, null);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void executeSqlWithoutResultSet() {
        IClient client = ClientCache.getClient(DataSourceType.GBase_8a.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableMetaComment() {
        IClient client = ClientCache.getClient(DataSourceType.GBase_8a.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertEquals("table comment", metaComment);
    }
}
