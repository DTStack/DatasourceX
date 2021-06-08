package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.FtpSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:02 2020/5/21
 * @Description：FTP 测试
 */
public class FtpTest extends BaseTest {
    FtpSourceDTO source = FtpSourceDTO.builder()
            .url("172.16.100.251")
            .hostPort("22")
            .username("root")
            .password("dt@sz.com")
            .protocol("sftp")
            .build();

    @Test
    public void testCon() throws Exception {
        IClient client = ClientCache.getClient(DataSourceType.FTP.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test(expected = DtLoaderException.class)
    public void testCon_1() throws Exception {
        FtpSourceDTO source = FtpSourceDTO.builder()
                .url("172.16.100.251")
                .username("root")
                .hostPort("22")
                .password("dt@sz.com")
                .build();
        IClient client = ClientCache.getClient(DataSourceType.FTP.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void testCon_2() throws Exception {
        FtpSourceDTO source = FtpSourceDTO.builder()
                .url("172.16.8.173")
                .protocol("ftp")
                .hostPort("21")
                .username("Administrator")
                .password("XN#passw0rd2019")
                .build();
        IClient client = ClientCache.getClient(DataSourceType.FTP.getVal());
        Boolean isConnected = client.testCon(source);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }
}
