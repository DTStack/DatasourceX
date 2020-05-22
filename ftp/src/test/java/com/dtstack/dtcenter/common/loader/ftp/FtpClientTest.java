package com.dtstack.dtcenter.common.loader.ftp;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import org.junit.Test;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 23:02 2020/2/27
 * @Description：FTP 客户端测试
 */
public class FtpClientTest {
    private static FtpClient ftpClient = new FtpClient();

    @Test
    public void testCon() throws Exception {
        SourceDTO source = SourceDTO.builder()
                .url("kudu1")
                .hostPort("22")
                .username("root")
                .password("abc123")
                .protocol("sftp")
                .build();
        Boolean isConnected = ftpClient.testCon(source);
        if (!isConnected) {
            throw new DtCenterDefException("数据源连接异常");
        }
    }
}

