package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.EMQSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

/**
 * Date: 2020/4/9
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class EMQTest {
    IClient client = ClientCache.getClient(DataSourceType.EMQ.getVal());
    EMQSourceDTO source = EMQSourceDTO.builder()
            .url("tcp://kudu5:1883")
            .build();

    @Test
    public void testCon() throws Exception {
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("连接异常");
        }
    }

    @Test(expected = DtLoaderException.class)
    public void getCon() throws Exception {
        client.getCon(source);
    }

    @Test(expected = DtLoaderException.class)
    public void executeQuery() throws Exception {
        client.executeQuery(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void executeSqlWithoutResultSet() throws Exception {
        client.executeSqlWithoutResultSet(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getTableList() throws Exception {
        client.getTableList(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnClassInfo() throws Exception {
        client.getColumnClassInfo(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaData() throws Exception {
        client.getColumnMetaData(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataWithSql() throws Exception {
        client.getColumnMetaDataWithSql(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData() throws Exception {
        client.getFlinkColumnMetaData(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getTableMetaComment() throws Exception {
        client.getTableMetaComment(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getPreview() throws Exception {
        client.getPreview(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getDownloader() throws Exception {
        client.getDownloader(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getAllDatabases() throws Exception {
        client.getAllDatabases(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getCreateTableSql() throws Exception {
        client.getCreateTableSql(source, null);
    }

    @Test(expected = DtLoaderException.class)
    public void getPartitionColumn() throws Exception {
        client.getPartitionColumn(source, null);
    }
}
