package com.dtstack.dtcenter.common.loader.ftps.common;

import com.dtstack.dtcenter.common.sftp.SFTPHandler;
import com.dtstack.dtcenter.common.util.AddressUtil;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.dto.*;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.kafka.common.requests.MetadataResponse;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:50 2020/2/27
 * @Description：FTP 抽象类
 */
@Slf4j
public abstract class AbsFtpClient implements IClient {
    private static final int TIMEOUT = 60000;

    @Override
    public Boolean testCon(SourceDTO source) {
        boolean check = false;
        if (source == null || !AddressUtil.telnet(source.getUrl(), Integer.valueOf(source.getHostPort()))) {
            return check;
        }
        if (source.getProtocol() != null && source.getProtocol().equalsIgnoreCase("sftp")) {
            SFTPHandler instance = null;
            try {
                Integer finalPort = Integer.valueOf(source.getHostPort());
                instance = SFTPHandler.getInstance(
                        new HashMap<String, String>() {{
                            put(SFTPHandler.KEY_HOST, source.getUrl());
                            put(SFTPHandler.KEY_PORT, String.valueOf(finalPort));
                            put(SFTPHandler.KEY_USERNAME, source.getUsername());
                            put(SFTPHandler.KEY_PASSWORD, source.getPassword());
                            put(SFTPHandler.KEY_TIMEOUT, String.valueOf(TIMEOUT));
                            put(SFTPHandler.KEY_AUTHENTICATION,
                                    Optional.ofNullable(source.getAuth()).orElse("").toString());
                            put(SFTPHandler.KEY_RSA, Optional.ofNullable(source.getPath()).orElse("").toString());
                        }});
                check = true;
            } catch (Exception e) {
                log.error("无法通过sftp与服务器建立链接，请检查主机名和用户名是否正确, {}", e);
            } finally {
                if (instance != null) {
                    instance.close();
                }
            }
        } else {
            if (StringUtils.isBlank(source.getHostPort())) {
                source.setHostPort("21");
            }
            FTPClient ftpClient = new FTPClient();
            try {
                ftpClient.connect(source.getUrl(), Integer.valueOf(source.getHostPort()));
                ftpClient.login(source.getUsername(), source.getPassword());
                ftpClient.setConnectTimeout(TIMEOUT);
                ftpClient.setDataTimeout(TIMEOUT);
                if ("PASV".equalsIgnoreCase(source.getConnectMode())) {
                    ftpClient.enterRemotePassiveMode();
                    ftpClient.enterLocalPassiveMode();
                } else if ("PORT".equals(source.getConnectMode())) {
                    ftpClient.enterLocalActiveMode();
                }
                int reply = ftpClient.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    ftpClient.disconnect();
                    String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                            "message:host =" + source.getUrl() + ",username = " + source.getUsername() + ",port =" + source.getPassword());
                    log.error(message);
                } else {
                    check = true;
                }

                if (ftpClient != null) {
                    ftpClient.disconnect();
                }
            } catch (Exception e) {
                log.error("无法通过ftp与服务器建立链接，请检查主机名和用户名是否正确");
            }
        }

        return check;
    }

    /********************************* FTP 数据库无需实现的方法 ******************************************/
    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getTableMetaComment(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getAllBrokersAddress(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTopicList(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean createTopic(SourceDTO source, KafkaTopicDTO kafkaTopic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<MetadataResponse.PartitionMetadata> getAllPartitions(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<KafkaOffsetDTO> getOffset(SourceDTO source, String topic) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}