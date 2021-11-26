package com.dtstack.dtcenter.loader.client.sql;

import com.dtstack.dtcenter.common.loader.ftp.FtpClientFactory;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.source.FtpSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 20:02 2020/5/21
 * @Description：FTP 测试
 */
public class FtpTest extends BaseTest {

    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.FTP.getVal());

    private static final FtpSourceDTO SFTP_SOURCE_DTO = FtpSourceDTO.builder()
            .url("172.16.100.251")
            .hostPort("22")
            .username("root")
            .password("dt@sz.com")
            .protocol("sftp")
            .build();

    private static final FtpSourceDTO FTP_SOURCE_DTO = FtpSourceDTO.builder()
            .url("172.16.100.251")
            .hostPort("21")
            .username("admin")
            .password("dt@sz.com")
            .protocol("ftp")
            .build();

    @Test
    public void testConSFTP() {
        Boolean isConnected = CLIENT.testCon(SFTP_SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void testConFTP() {
        Boolean isConnected = CLIENT.testCon(FTP_SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }

    @Test
    public void listSftpFileNames() {
        Assert.assertEquals(3, CLIENT.listFileNames(SFTP_SOURCE_DTO, "/tmp", true, true, 3, ".*").size());
        Assert.assertEquals(3, CLIENT.listFileNames(SFTP_SOURCE_DTO, "/tmp", false, false, 3, ".*").size());
        Assert.assertEquals(3, CLIENT.listFileNames(SFTP_SOURCE_DTO, "/tmp", true, false, 3, ".*").size());
        Assert.assertEquals(3, CLIENT.listFileNames(SFTP_SOURCE_DTO, "/tmp", false, true, 3, ".*").size());
    }

    @Test
    public void listFtpFileNames() {
        Assert.assertEquals(3, CLIENT.listFileNames(FTP_SOURCE_DTO, "/tmp", true, true, 3, ".*").size());
        Assert.assertEquals(3, CLIENT.listFileNames(FTP_SOURCE_DTO, "/tmp", false, false, 3, ".*").size());
        Assert.assertEquals(3, CLIENT.listFileNames(FTP_SOURCE_DTO, "/tmp", true, false, 3, ".*").size());
        Assert.assertEquals(3, CLIENT.listFileNames(FTP_SOURCE_DTO, "/tmp", false, true, 3, ".*").size());
    }

    @Test
    public void list() throws IOException {
        List<String> ftpFiles = CLIENT.listFileNames(FTP_SOURCE_DTO, "/tmp/ftp_test", true, true, 1000, ".*");
        for (String ftpFile : ftpFiles) {
            if ("/tmp/ftp_test/千一.txt".contains(ftpFile)) {
                return;
            }
        }

        assert false;

    }
}
