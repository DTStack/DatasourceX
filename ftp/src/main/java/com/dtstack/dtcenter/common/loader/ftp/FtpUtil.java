package com.dtstack.dtcenter.common.loader.ftp;

import com.google.common.collect.Sets;
import com.jcraft.jsch.ChannelSftp;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

/**
 * ftp 工具类
 *
 * @author ：wangchuan
 * date：Created in 下午4:11 2021/6/21
 * company: www.dtstack.com
 */
@Slf4j
public class FtpUtil {

    /**
     * FTP 默认端口
     */
    private static final Integer FTP_PORT_DEFAULT = 21;

    /**
     * SFTP 默认端口
     */
    private static final Integer SFTP_PORT_DEFAULT = 22;

    /**
     * 获取 ftp/sftp 端口
     *
     * @param protocol 协议类型
     * @param portStr  String 类型端口
     * @return ftp/sftp 端口
     */
    public static Integer getFtpPort(String protocol, String portStr) {
        if (StringUtils.equalsIgnoreCase(ProtocolEnum.SFTP.name(), protocol)) {
            return StringUtils.isNotBlank(portStr) ? Integer.valueOf(portStr) : SFTP_PORT_DEFAULT;
        } else {
            return StringUtils.isNotBlank(portStr) ? Integer.valueOf(portStr) : FTP_PORT_DEFAULT;
        }
    }

    /**
     * 获取 SFTP 上的文件集合
     *
     * @param handler    SFTP Client
     * @param path       地址
     * @param includeDir 是否包含文件夹
     * @param maxNum     最大条数
     * @param regexStr   正则匹配
     * @return 文件名集合
     */
    public static List<String> getSFTPFileNames(SFTPHandler handler, String path, Boolean includeDir, Boolean recursive, Integer maxNum, String regexStr) {
        return getSFTPFileNamesByPath(handler, path, includeDir, recursive, maxNum, Sets.newHashSet(), regexStr);
    }

    /**
     * 获取 FTP 上的文件集合
     *
     * @param ftpClient  FTP client
     * @param path       地址
     * @param includeDir 是否包含文件夹
     * @param maxNum     最大条数
     * @param regexStr   正则匹配
     * @return 文件名集合
     */
    public static List<String> getFTPFileNames(FTPClient ftpClient, String path, Boolean includeDir, Boolean recursive, Integer maxNum, String regexStr) {
        return getFTPFileNamesByPath(ftpClient, path, includeDir, recursive, maxNum, Sets.newHashSet(), regexStr);
    }

    private static List<String> getSFTPFileNamesByPath(SFTPHandler handler, String path, Boolean includeDir, Boolean recursive, Integer maxNum, Set<String> fileNames, String regexStr) {
        if (handler == null) {
            return new ArrayList<>();
        }
        try {
            Vector vector = handler.listFile(path);
            for (Object single : vector) {
                ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) single;
                if (lsEntry.getAttrs().isDir() && !(lsEntry.getFilename().equals(".") || lsEntry.getFilename().equals(".."))) {
                    if (includeDir) {
                        if (fileNames.size() == maxNum) {
                            break;
                        }
                        listAddByRegex(fileNames, regexStr, lsEntry.getFilename(), path);
                    }
                    if (recursive) {
                        getSFTPFileNamesByPath(handler, path + "/" + lsEntry.getFilename(), includeDir, true, maxNum, fileNames, regexStr);
                    }
                } else if (!lsEntry.getAttrs().isDir()) {
                    if (fileNames.size() == maxNum) {
                        break;
                    }
                    listAddByRegex(fileNames, regexStr, lsEntry.getFilename(), path);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            handler.close();
        }
        return new ArrayList<>(fileNames);
    }

    private static void listAddByRegex(Set<String> fileNames, String regexStr, String fileName, String namePrefix) {
        if (StringUtils.isBlank(regexStr) || Pattern.compile(regexStr).matcher(fileName).matches()) {
            fileNames.add((namePrefix + "/" + fileName).replaceAll("//*", "/"));
        }
    }

    private static List<String> getFTPFileNamesByPath(FTPClient ftpClient, String path, Boolean includeDir, Boolean recursive, Integer maxNum, Set<String> fileNames, String regexStr) {
        if (ftpClient == null) {
            return new ArrayList<>();
        }
        try {
            FTPFile[] ftpFiles = ftpClient.listFiles(path);
            if (ArrayUtils.isEmpty(ftpFiles)) {
                return Collections.emptyList();
            }
            for (FTPFile file : ftpFiles) {
                if (file.isDirectory() && !(file.getName().equals(".") || file.getName().equals(".."))) {
                    if (includeDir) {
                        if (fileNames.size() == maxNum) {
                            break;
                        }
                        listAddByRegex(fileNames, regexStr, file.getName(), path);
                    }
                    if (recursive) {
                        getFTPFileNamesByPath(ftpClient, path + "/" + file.getName(), includeDir, true, maxNum, fileNames, regexStr);
                    }
                } else if (!file.isDirectory()) {
                    if (fileNames.size() == maxNum) {
                        break;
                    }
                    listAddByRegex(fileNames, regexStr, file.getName(), path);
                }
            }
            return new ArrayList<>(fileNames);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return new ArrayList<>(fileNames);
    }
}
