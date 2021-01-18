package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.OdpsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 09:52 2020/4/24
 * @Description：MaxComputer 测试
 */
public class MaxComputerTest {
    
    // 构建client
    private static final IClient client = ClientCache.getClient(DataSourceType.MAXCOMPUTE.getVal());
    
    // 构建数据源信息 ps：em没有对应的数据源类型
    private static final OdpsSourceDTO source = OdpsSourceDTO.builder()
            .config("{\"accessId\":\"LTAIljBeC8ei9Yy0\",\"accessKey\":\"gwTWasH7sEE0pSUEuiXnw7JecXyfGF\"," +
                    "\"project\":\"dtstack_dev\",\"endPoint\":\"https://service.odps.aliyun.com/api\"}")
            .poolConfig(new PoolConfig())
            .build();

    /**
     * 数据准备
     */
    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("create table loader_test( id STRING COMMENT '工作类型', name STRING COMMENT '婚否') COMMENT 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test values ('1', 'loader_test')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    /**
     * 简单查询
     */
    @Test
    public void executeQuery() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test;").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    /**
     * 无需结果查询
     */
    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("show tables").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    /**
     * 获取表列表
     */
    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    /**
     * 获取表字段标准 java 类型
     */
    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    /**
     * 获取表字段详细信息
     */
    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    /**
     * 获取表注释
     */
    @Test
    public void getTableMetaComment() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(metaComment));
    }

    /**
     * 数据预览
     */
    @Test
    public void getPreview() {
        HashMap<String, String> map = new HashMap<>();
        map.put("id", "1");
        List data = client.getPreview(source, SqlQueryDTO.builder().partitionColumns(map).previewNum(1000).tableName("loader_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(data));
    }

    /**
     * 根据sql获取字段信息
     */
    @Test
    public void getColumnMetaDataWithSql() {
        IClient client = ClientCache.getClient(DataSourceType.MAXCOMPUTE.getVal());
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test").build();
        List data = client.getColumnMetaDataWithSql(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(data));
    }
}
