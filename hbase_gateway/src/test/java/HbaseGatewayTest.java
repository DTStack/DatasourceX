import com.dtstack.dtcenter.common.loader.hbase.HbaseClient;
import com.dtstack.dtcenter.loader.cache.pool.config.PoolConfig;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.comparator.BinaryComparator;
import com.dtstack.dtcenter.loader.dto.filter.Filter;
import com.dtstack.dtcenter.loader.dto.filter.RowFilter;
import com.dtstack.dtcenter.loader.dto.source.HbaseSourceDTO;
import com.dtstack.dtcenter.loader.enums.CompareOp;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HbaseGatewayTest {
    // 构建hbase client
    private static final HbaseClient hbaseClient =  new HbaseClient();

    // 连接池信息
    private static final PoolConfig poolConfig = PoolConfig.builder().maximumPoolSize(100).build();

    // 构建数据源信息
    private static final HbaseSourceDTO source = HbaseSourceDTO.builder()
            .url("172.16.100.175:2181,172.16.101.196:2181,172.16.101.227:2181")
            .path("/hbase2")
            .poolConfig(poolConfig)
            .build();

    /**
     * 测试
     */
    @Test
    public void testCon() {
        Boolean check = hbaseClient.testCon(source);
        Assert.assertTrue(check);
    }

    /**
     * 获取表
     */
    @Test
    public void getTableList() {
        hbaseClient.getTableList(source, SqlQueryDTO.builder().build());
    }


    @Test
    public void getColumnMetaData() {
        List metaData = hbaseClient.getColumnMetaData(source, SqlQueryDTO.builder().tableName("loader_test").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(metaData));
    }

    @Test
    public void executeQuery() {
        ArrayList<Filter> filters = new ArrayList<>();
        RowFilter hbaseRowFilter = new RowFilter(
                CompareOp.GREATER, new BinaryComparator("0".getBytes()));
        hbaseRowFilter.setReversed(true);
        filters.add(hbaseRowFilter);
        SqlQueryDTO sqlQueryDTO = SqlQueryDTO.builder().tableName("loader_test").hbaseFilter(filters).build();
        List result = hbaseClient.executeQuery(source, sqlQueryDTO);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
    }

    /**
     * 测试不存在的namespace
     */
    @Test(expected = DtLoaderException.class)
    public void dbNotExists() {
        Boolean check = hbaseClient.isDatabaseExists(source, UUID.randomUUID().toString());
        Assert.assertFalse(check);
    }

    /**
     * 创建已经存在的表测试
     */
    @Test(expected = DtLoaderException.class)
    public void createHbaseTableExists() {
        Boolean check =  hbaseClient.createDatabase(source, "loader_test_2","info1");
        Assert.assertFalse(check);
    }

    @Test(expected = DtLoaderException.class)
    public void getCon() {
        hbaseClient.getCon(source);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataWithSql() {
        hbaseClient.getColumnMetaDataWithSql(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData() {
        hbaseClient.getFlinkColumnMetaData(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTableListBySchema()  {
        hbaseClient.getTableListBySchema(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTableMetaComment() {
        hbaseClient.getTableMetaComment(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void executeSqlWithoutResultSet() {
        hbaseClient.executeSqlWithoutResultSet(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnClassInfo() {
        hbaseClient.getColumnClassInfo(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getDownloader() {
        hbaseClient.getDownloader(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getAllDatabases() {
        hbaseClient.getAllDatabases(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getCreateTableSql() {
        hbaseClient.getCreateTableSql(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getPartitionColumn() {
        hbaseClient.getPartitionColumn(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTable() {
        hbaseClient.getTable(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getCurrentDatabase()  {
        hbaseClient.getCurrentDatabase(source );
    }

    @Test(expected = DtLoaderException.class)
    public void createDatabase() {
        hbaseClient.createDatabase(source, "","");
    }

    @Test(expected = DtLoaderException.class)
    public void isDatabaseExists() {
        hbaseClient.isDatabaseExists(source, "");
    }

    @Test(expected = DtLoaderException.class)
    public void isTableExistsInDatabase() {
        hbaseClient.isTableExistsInDatabase(source, "", "");
    }
}
