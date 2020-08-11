package com.dtstack.dtcenter.common.loader.hdfs;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hdfs.downloader.YarnDownload;
import com.dtstack.dtcenter.loader.DtClassConsistent;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.IHdfsWriter;
import com.dtstack.dtcenter.loader.client.IHdfsFile;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;

import java.security.PrivilegedAction;
import java.util.List;
import java.util.Properties;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:50 2020/8/10
 * @Description：HDFS 文件操作实现类
 */
public class HdfsFileClient implements IHdfsFile {
    @Override
    public FileStatus getStatus(ISourceDTO iSource, String location) throws Exception {
        org.apache.hadoop.fs.FileStatus hadoopFileStatus = getHadoopStatus(iSource, location);

        return FileStatus.builder()
                .length(hadoopFileStatus.getLen())
                .access_time(hadoopFileStatus.getAccessTime())
                .block_replication(hadoopFileStatus.getReplication())
                .blocksize(hadoopFileStatus.getBlockSize())
                .group(hadoopFileStatus.getGroup())
                .isdir(hadoopFileStatus.isDirectory())
                .modification_time(hadoopFileStatus.getModificationTime())
                .owner(hadoopFileStatus.getOwner())
                .build();
    }

    @Override
    public IDownloader getLogDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;

        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType());
            yarnDownload.configure();
            return yarnDownload;
        }

        // 校验高可用配置
        return KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<IDownloader>) () -> {
                    try {
                        YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType());
                        yarnDownload.configure();
                        return yarnDownload;
                    } catch (Exception e) {
                        throw new DtCenterDefException("创建下载器异常", e);
                    }
                }
        );
    }

    /**
     * 获取 HADOOP 文件信息
     *
     * @param iSource
     * @param location
     * @return
     * @throws Exception
     */
    private org.apache.hadoop.fs.FileStatus getHadoopStatus(ISourceDTO iSource, String location) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        if (!hdfsSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtCenterDefException("defaultFS格式不正确");
        }

        Properties properties = HdfsConnFactory.combineHdfsConfig(hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
        Configuration conf = new HdfsOperator.HadoopConf().setConf(hdfsSourceDTO.getDefaultFS(), properties);
        //不在做重复认证 主要用于 HdfsOperator.checkConnection 中有一些数栈自己的逻辑
        conf.set("hadoop.security.authorization", "false");
        conf.set("dfs.namenode.kerberos.principal.pattern", "*");

        org.apache.hadoop.fs.FileStatus hadoopFileStatus = null;
        if (MapUtils.isEmpty(hdfsSourceDTO.getKerberosConfig())) {
            hadoopFileStatus = HdfsOperator.getFileStatus(conf, location);
        }

        hadoopFileStatus = KerberosUtil.loginKerberosWithUGI(hdfsSourceDTO.getKerberosConfig()).doAs(
                (PrivilegedAction<org.apache.hadoop.fs.FileStatus>) () -> {
                    try {
                        return HdfsOperator.getFileStatus(conf, location);
                    } catch (Exception e) {
                        throw new DtCenterDefException("获取 hdfs 文件状态异常", e);
                    }
                }
        );

        return hadoopFileStatus;
    }

    @Override
    public boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir) throws Exception {
        return false;
    }

    @Override
    public boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean createDir(ISourceDTO source, String remotePath, Short permission) throws Exception {
        return false;
    }

    @Override
    public boolean isFileExist(ISourceDTO source, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean checkAndDelete(ISourceDTO source, String remotePath) throws Exception {
        return false;
    }

    @Override
    public long getDirSize(ISourceDTO source, String remotePath) throws Exception {
        return 0;
    }

    @Override
    public boolean deleteFiles(ISourceDTO source, List<String> fileNames) throws Exception {
        return false;
    }

    @Override
    public boolean isDirExist(ISourceDTO source, String remotePath) throws Exception {
        return false;
    }

    @Override
    public boolean setPermission(ISourceDTO source, String remotePath, String mode) throws Exception {
        return false;
    }

    @Override
    public boolean rename(ISourceDTO source, String src, String dist) throws Exception {
        return false;
    }

    @Override
    public boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite) throws Exception {
        return false;
    }

    @Override
    public List<String> listAllFilePath(ISourceDTO source, String remotePath) throws Exception {
        return null;
    }

    @Override
    public List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate) throws Exception {
        return null;
    }

    @Override
    public boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath) throws Exception {
        return false;
    }

    @Override
    public boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite) throws Exception {
        return false;
    }

    @Override
    public boolean copyInHDFS(ISourceDTO source1, ISourceDTO source2, String srcPath, String dstPath, String localPath, boolean overwrite) throws Exception {
        return false;
    }

    @Override
    public IHdfsWriter getHdfsWriter(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws Exception {
        return null;
    }

    @Override
    public IDownloader getDownloaderByFormat(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat) throws Exception {
        return null;
    }
}
