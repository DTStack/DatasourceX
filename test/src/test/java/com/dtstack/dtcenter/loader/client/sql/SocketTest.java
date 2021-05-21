package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.SocketSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * socket数据源测试
 *
 * @author ：wangchuan
 * date：Created in 4:16 下午 2020/12/28
 * company: www.dtstack.com
 */
public class SocketTest extends BaseTest {

    public static final ExecutorService ES = Executors.newSingleThreadExecutor();

    /**
     * 初始化socket服务端
     */
    @BeforeClass
    public static void setUp() {
        ES.submit(() -> {
            try {
                // 创建socket服务端，端口为15000
                ServerSocket serverSocket = new ServerSocket(15000);
                //此方法为阻塞方法，用户等待客户端连接
                Socket socket = serverSocket.accept();
                InputStream inputStream = socket.getInputStream();
                InputStreamReader in = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(in);
                String info = reader.readLine();
                System.out.println("客户端说：" + info);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(client.testCon(source));
    }

    /**
     * 连通性失败测试 - 错误端口
     */
    @Test(expected = DtLoaderException.class)
    public void testConPortError() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15001").build();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(client.testCon(source));
    }

    /**
     * 连通性失败测试 - 不存在的域名
     */
    @Test(expected = DtLoaderException.class)
    public void testConUnKnowHost() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost123:15001").build();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(client.testCon(source));
    }

    /**
     * 连通性失败测试 - 错误ip ，跳过 太费时间
     */
    @Ignore
    @Test(expected = DtLoaderException.class)
    public void testConIpError() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("1.1.1.1:15000").build();
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(client.testCon(source));
    }

    /**
     * 清理资源
     */
    @AfterClass
    public static void clean() {
        // 关闭线程池
        ES.shutdownNow();
    }

    /********************************* 非关系型数据库无需实现的方法 ******************************************/
    @Test(expected = DtLoaderException.class)
    public void getCon() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getCon(source);
    }

    @Test(expected = DtLoaderException.class)
    public void executeQuery() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.executeQuery(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void executeSqlWithoutResultSet() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.executeSqlWithoutResultSet(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTableList() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getTableList(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTableListBySchema() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getTableListBySchema(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnClassInfo() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getColumnClassInfo(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaData() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getColumnMetaData(source, SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getColumnMetaDataWithSql() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getColumnMetaDataWithSql(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getFlinkColumnMetaData() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getFlinkColumnMetaData(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTableMetaComment() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getTableMetaComment(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getPreview() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getPreview(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getDownloader() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getDownloader(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getAllDatabases() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getAllDatabases(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getCreateTableSql() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getCreateTableSql(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getPartitionColumn() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getPartitionColumn(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getTable() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getTable(source,SqlQueryDTO.builder().build());
    }

    @Test(expected = DtLoaderException.class)
    public void getCurrentDatabase() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.getCurrentDatabase(source);
    }

    @Test(expected = DtLoaderException.class)
    public void createDatabase() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.createDatabase(source,"","");
    }

    @Test(expected = DtLoaderException.class)
    public void isDatabaseExists() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.isDatabaseExists(source,"");
    }

    @Test(expected = DtLoaderException.class)
    public void isTableExistsInDatabase() {
        IClient client = ClientCache.getClient(DataSourceType.SOCKET.getVal());
        SocketSourceDTO source = SocketSourceDTO.builder().hostPort("localhost:15000").build();
        client.isTableExistsInDatabase(source,"","");
    }
}
