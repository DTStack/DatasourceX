package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DmSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：loader_test
 * @Date ：Created in 13:49 2020/4/17
 * @Description：达梦数据源测试
 */
@Slf4j
@Ignore
public class DmDbTest extends BaseTest {

    private static final IClient client = ClientCache.getClient(DataSourceType.DMDB.getVal());

    private static final DmSourceDTO source = DmSourceDTO.builder()
            .url("jdbc:dm://172.16.100.199/DAMENG")
            .username("SYSDBA")
            .password("adminabc123")
            .poolConfig(PoolConfig.builder().build())
            .build();

    @BeforeClass
    public static void beforeClass() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("drop table if exists loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);

        queryDTO = SqlQueryDTO.builder().sql("create table loader_test (id int, name varchar(50))").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("comment on table loader_test is 'table comment'").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
        queryDTO = SqlQueryDTO.builder().sql("insert into loader_test values (1, 'loader_test')").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getCon() throws Exception{
        Connection con = client.getCon(source);
        con.createStatement().close();
        con.close();
    }

    @Test
    public void testCon() {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void executeQuery() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test limit 8").build();
        List<Map<String, Object>> mapList = client.executeQuery(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(mapList));
    }

    @Test
    public void executeSqlWithoutResultSet() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test").build();
        client.executeSqlWithoutResultSet(source, queryDTO);
    }

    @Test
    public void getTableList() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List<String> tableList = client.getTableList(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
    }

    @Test
    public void getColumnClassInfo() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<String> columnClassInfo = client.getColumnClassInfo(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnClassInfo));
    }

    @Test
    public void getColumnMetaData() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        List<ColumnMetaDTO> columnMetaData = client.getColumnMetaData(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(columnMetaData));
    }

    @Test
    public void getTableMetaComment() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        String metaComment = client.getTableMetaComment(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(metaComment));
    }

    @Test
    public void getDownloader() throws Exception {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().sql("select * from loader_test").build();
        IDownloader downloader = client.getDownloader(source, queryDTO);
        if (!downloader.reachedEnd()) {
            List<List<String>> result = (List<List<String>>) downloader.readNext();
            for (List<String> row : result) {
                Assert.assertTrue(CollectionUtils.isNotEmpty(row));
            }
        }
    }

    @Test
    public void getAllDatabases() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().build();
        List databases = client.getAllDatabases(source, queryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(databases));
    }

    @Test
    public void getCreateTableSql() {
        SqlQueryDTO queryDTO = SqlQueryDTO.builder().tableName("loader_test").build();
        String createTableSql = client.getCreateTableSql(source, queryDTO);
        Assert.assertTrue(StringUtils.isNotBlank(createTableSql));
    }

    /**
     * 获取版本
     */
    @Test
    public void getVersion() {
        Assert.assertTrue(StringUtils.isNotBlank(client.getVersion(source)));
    }
}
