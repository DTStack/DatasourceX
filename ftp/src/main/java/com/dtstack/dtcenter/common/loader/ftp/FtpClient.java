package com.dtstack.dtcenter.common.loader.ftp;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.dto.source.FtpSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;

import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 22:52 2020/2/27
 * @Description：FTP 客户端
 */
public class FtpClient<T> extends AbsNoSqlClient<T> {

    @Override
    public Boolean testCon(ISourceDTO sourceDTO) {
        FtpSourceDTO ftpSourceDTO = (FtpSourceDTO) sourceDTO;
        Integer port = FtpUtil.getFtpPort(ftpSourceDTO.getProtocol(), ftpSourceDTO.getHostPort());
        if (!AddressUtil.telnet(ftpSourceDTO.getUrl(), port)) {
            return Boolean.FALSE;
        }
        if (StringUtils.equalsIgnoreCase(ProtocolEnum.SFTP.name(), ftpSourceDTO.getProtocol())) {
            SFTPHandler sftpHandler = FtpClientFactory.getSFTPHandler(ftpSourceDTO);
            sftpHandler.close();
        } else {
            try {
                FTPClient ftpClient = FtpClientFactory.getFtpClient(ftpSourceDTO);
                ftpClient.disconnect();
            } catch (Exception e) {
                throw new DtLoaderException(e.getMessage(), e);
            }
        }
        return true;
    }

    @Override
    public List<String> listFileNames(ISourceDTO sourceDTO, String path, Boolean includeDir, Boolean recursive, Integer maxNum, String regexStr) {
        FtpSourceDTO ftpSourceDTO = (FtpSourceDTO) sourceDTO;
        List<String> fileNames;
        if (StringUtils.equalsIgnoreCase(ProtocolEnum.SFTP.name(), ftpSourceDTO.getProtocol())) {
            fileNames = FtpUtil.getSFTPFileNames(FtpClientFactory.getSFTPHandler(ftpSourceDTO), path, includeDir, recursive, maxNum, regexStr);
        } else {
            fileNames = FtpUtil.getFTPFileNames(FtpClientFactory.getFtpClient(ftpSourceDTO), path, includeDir, recursive, maxNum, regexStr);
        }
        fileNames.sort(String::compareTo);
        return fileNames;
    }
}
