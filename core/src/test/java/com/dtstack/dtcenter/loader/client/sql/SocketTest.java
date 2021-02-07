package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
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
public class SocketTest {

    public static final ExecutorService ES = Executors.newSingleThreadExecutor();

    /**
     * 初始化socket服务端
     */
    @BeforeClass
    public static void setUp () {
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
    public static void clean () {
        // 关闭线程池
        ES.shutdownNow();
    }
}
