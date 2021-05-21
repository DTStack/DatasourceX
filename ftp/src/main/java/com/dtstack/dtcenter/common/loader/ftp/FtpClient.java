package com.dtstack.dtcenter.common.loader.ftp;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.dto.source.FtpSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Maps;
import com.jcraft.jsch.JSchException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.util.Map;
import java.util.Optional;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:52 2020/2/27
 * @Description：FTP 客户端
 */
public class FtpClient<T> extends AbsNoSqlClient<T> {

    private static final int TIMEOUT = 60000;

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        FtpSourceDTO ftpSourceDTO = (FtpSourceDTO) iSource;
        if (ftpSourceDTO == null || !AddressUtil.telnet(ftpSourceDTO.getUrl(), Integer.valueOf(ftpSourceDTO.getHostPort()))) {
            return Boolean.FALSE;
        }

        if (ftpSourceDTO.getProtocol() != null && "sftp".equalsIgnoreCase(ftpSourceDTO.getProtocol())) {
            SFTPHandler instance = null;
            try {
                Integer finalPort = Integer.valueOf(ftpSourceDTO.getHostPort());
                Map<String, String> sftpConfig = Maps.newHashMap();
                sftpConfig.put(SFTPHandler.KEY_HOST, ftpSourceDTO.getUrl());
                sftpConfig.put(SFTPHandler.KEY_PORT, String.valueOf(finalPort));
                sftpConfig.put(SFTPHandler.KEY_USERNAME, ftpSourceDTO.getUsername());
                sftpConfig.put(SFTPHandler.KEY_PASSWORD, ftpSourceDTO.getPassword());
                sftpConfig.put(SFTPHandler.KEY_TIMEOUT, String.valueOf(TIMEOUT));
                sftpConfig.put(SFTPHandler.KEY_AUTHENTICATION, Optional.ofNullable(ftpSourceDTO.getAuth()).orElse(""));
                sftpConfig.put(SFTPHandler.KEY_RSA, Optional.ofNullable(ftpSourceDTO.getPath()).orElse(""));
                instance = SFTPHandler.getInstance(sftpConfig);
            } catch (JSchException e) {
                throw new DtLoaderException("Failed to establish a connection with the ftp server, please check whether the user name and password are correct");
            } finally {
                if (instance != null) {
                    instance.close();
                }
            }
        } else {
            if (StringUtils.isBlank(ftpSourceDTO.getHostPort())) {
                ftpSourceDTO.setHostPort("21");
            }
            FTPClient ftpClient = new FTPClient();
            try {
                ftpClient.connect(ftpSourceDTO.getUrl(), Integer.valueOf(ftpSourceDTO.getHostPort()));
                ftpClient.login(ftpSourceDTO.getUsername(), ftpSourceDTO.getPassword());
                ftpClient.setConnectTimeout(TIMEOUT);
                ftpClient.setDataTimeout(TIMEOUT);
                if ("PASV".equalsIgnoreCase(ftpSourceDTO.getConnectMode())) {
                    ftpClient.enterRemotePassiveMode();
                    ftpClient.enterLocalPassiveMode();
                } else if ("PORT".equals(ftpSourceDTO.getConnectMode())) {
                    ftpClient.enterLocalActiveMode();
                }
                int reply = ftpClient.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    ftpClient.disconnect();
                    throw new DtLoaderException("Failed to establish a connection with the ftp server, please check whether the user name and password are correct");
                }

                if (ftpClient != null) {
                    ftpClient.disconnect();
                }
            } catch (Exception e) {
                throw new DtLoaderException("Failed to establish a connection with the ftp server, please check whether the user name and password are correct");
            }
        }
        return true;
    }
}
