package com.dtstack.dtcenter.common.loader.hadoop.hdfs;

import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:08 2020/9/1
 * @Description：Hdfs 操作
 */
@Slf4j
public class HdfsOperator {

    private static final IErrorPattern ERROR_PATTERN = new HDFSErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    /**
     * HDFS 文件路径正则
     */
    private static Pattern pattern = Pattern.compile("(hdfs://[^/]+)(.*)");

    /**
     * 确认连通性
     *
     * @param kerberosConfig
     * @param config
     * @param defaultFS
     * @return
     */
    public static boolean checkConnection(String defaultFS, String config, Map<String, Object> kerberosConfig) {
        try {
            FileSystem fs = getFileSystem(kerberosConfig, config, defaultFS);
            fs.getStatus(new Path("/"));
            return Boolean.TRUE;
        } catch (Exception e) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(e.getMessage(), ERROR_PATTERN), e);
        }
    }

    /**
     * 获取 Hdfs FileSystem 信息
     * 它自己本身有一个 Hook 线程去关闭 FileSystem
     * 平台这种不频繁且之前也没有关闭没出现问题（tfs 里面之前压根没往里面写），所以暂时不关闭
     *
     * @param kerberosConfig
     * @param config
     * @param defaultFS
     * @return
     * @throws IOException
     */
    public static FileSystem getFileSystem(Map<String, Object> kerberosConfig, String config, String defaultFS) {
        Configuration conf = HadoopConfUtil.getHdfsConf(defaultFS, config, kerberosConfig);
        log.info("get Hdfs FileSystem message, defaultFS : {}, config : {}, kerberosConfig : {}", defaultFS, config, kerberosConfig);
        return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<FileSystem>) () -> {
                    try {
                        return FileSystem.get(conf);
                    } catch (IOException e) {
                        throw new DtLoaderException("Hdfs check connect error", e);
                    }
                }
        );
    }

    /**
     * 获取 Config 信息
     *
     * @param kerberosConfig
     * @param config
     * @param defaultFS
     * @return
     * @throws IOException
     */
    public static Configuration getConfig(Map<String, Object> kerberosConfig, String config, String defaultFS) throws IOException {
        return HadoopConfUtil.getHdfsConf(defaultFS, config, kerberosConfig);
    }

    /**
     * 根据 FileSystem 获取 文件状态
     *
     * @param fs
     * @param location
     * @return
     */
    public static FileStatus getFileStatus(FileSystem fs, String location) {
        log.info("Hdfs get {} fileStatus;", location);
        try {
            return fs.getFileStatus(new Path(location));
        } catch (IOException e) {
            throw new DtLoaderException(String.format("get hdfs  file status exception : %s", e.getMessage()), e);
        }
    }

    /**
     * 复制文件到本地
     *
     * @param fs
     * @param remotePath
     * @param localFilePath
     * @return
     */
    public static boolean copyToLocal(FileSystem fs, String remotePath, String localFilePath) {
        log.info("copy HDFS file : {} to local : {}", remotePath, localFilePath);
        try {
            fs.copyToLocalFile(false, new Path(remotePath), new Path(localFilePath));
        } catch (Exception e) {
            throw new DtLoaderException(String.format("copy HDFS file to local exception : %s", e.getMessage()), e);
        }
        return true;
    }

    /**
     * 从本地复制文件到 HDFS 会删除 HDFS 的文件
     *
     * @param fs
     * @param localDir
     * @param remotePath
     * @param overwrite
     * @return
     */
    public static boolean copyFromLocal(FileSystem fs, String localDir, String remotePath, boolean overwrite) {
        log.info("from local : {} copy file to HDFS : {}", localDir, remotePath);
        try {
            fs.copyFromLocalFile(true, overwrite, new Path(localDir), new Path(remotePath));
        } catch (IOException e) {
            throw new DtLoaderException(String.format("copying files from local to HDFS error : %s", e.getMessage()), e);
        }
        return true;
    }

    /**
     * 上传对应路径的文件到 HDFS
     *
     * @param fs
     * @param localFilePath
     * @param remotePath
     */
    public static void uploadLocalFileToHdfs(FileSystem fs, String localFilePath, String remotePath) {
        log.info("upload file : {} to HDFS : {}", localFilePath, remotePath);
        Path resP = new Path(localFilePath);
        Path destP = new Path(remotePath);
        String dir = remotePath.substring(0, remotePath.lastIndexOf("/") + 1);
        try {
            if (!isDirExist(fs, dir)) {
                fs.mkdirs(new Path(dir));
            }
            fs.copyFromLocalFile(resP, destP);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("upload local file to HDFS exception : %s", e.getMessage()), e);
        }
    }

    /**
     * 上传字节流到 HDFS
     *
     * @param fs
     * @param bytes
     * @param remotePath
     * @return
     */
    public static boolean uploadInputStreamToHdfs(FileSystem fs, byte[] bytes, String remotePath) {
        log.info("upload byte stream to HDFS : {}", remotePath);
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes)) {
            Path destP = new Path(remotePath);
            FSDataOutputStream os = fs.create(destP);
            IOUtils.copyBytes(is, os, 4096, true);
        } catch (Exception e) {
            throw new DtLoaderException(String.format("upload byte stream to HDFS exception : %s", e.getMessage()), e);
        }
        return true;
    }

    /**
     * 复制文件
     *
     * @param fs
     * @param remotePath
     * @param distPath
     * @param isOverwrite
     * @return
     */
    public static boolean copyFile(FileSystem fs, String remotePath, String distPath, boolean isOverwrite) throws IOException {
        log.info("copy HDFS {} file {}", remotePath, distPath);
        Path remote = new Path(remotePath);
        Path dis = new Path(distPath);
        if (fs.isDirectory(remote)) {
            throw new DtLoaderException("Cannot copy directory");
        } else if (fs.isDirectory(dis)) {
            throw new DtLoaderException("Copy destination cannot be a directory");
        } else {
            if (isOverwrite) {
                try (FSDataInputStream in = fs.open(remote);) {
                    FSDataOutputStream os = fs.create(dis);
                    IOUtils.copyBytes(in, os, 4096, true);
                } catch (Exception e) {
                    throw e;
                }
            } else if (fs.exists(dis)) {
                throw new DtLoaderException("file：" + distPath + " is exits");
            }
        }
        return true;
    }

    /**
     * 新建目录
     *
     * @param fs
     * @param remotePath
     * @param permission
     * @return
     */
    public static boolean createDir(FileSystem fs, String remotePath, Short permission) {
        log.info("create dir HDFS : {}", remotePath);
        remotePath = uri(remotePath);
        try {
            if (null == permission) {
                return fs.mkdirs(new Path(remotePath));
            }
            return fs.mkdirs(new Path(remotePath), new FsPermission(permission));
        } catch (Exception e) {
            throw new DtLoaderException(String.format("create hdfs dir exception : %s", e.getMessage()), e);
        }
    }

    /**
     * 校验文件夹是否存在
     *
     * @param fs
     * @param dir
     * @return
     */
    public static boolean isDirExist(FileSystem fs, String dir) {
        log.info("Check the hdfs dir is exits : {}", dir);
        dir = uri(dir);
        Path path = new Path(dir);
        try {
            return fs.exists(path) && fs.isDirectory(path);
        } catch (IOException e) {
            throw new DtLoaderException(String.format("Check the hdfs dir is exits : %s", e.getMessage()), e);
        }
    }

    /**
     * 校验文件是否存在
     *
     * @param fs
     * @param remotePath
     * @return
     */
    public static boolean isFileExist(FileSystem fs, String remotePath) {
        log.info("Check the hdfs file is exits : {}", remotePath);
        remotePath = uri(remotePath);
        Path path = new Path(remotePath);
        try {
            return fs.exists(path) || fs.isFile(path);
        } catch (IOException e) {
            throw new DtLoaderException(String.format("Check the hdfs file is exits : %s", e.getMessage()), e);
        }
    }

    /**
     * HDFS 路径配置
     *
     * @param path
     * @return
     */
    public static String uri(String path) {
        Pair<String, String> pair = parseHdfsUri(path);
        path = pair == null ? path : pair.getRight();
        return path;
    }

    /**
     * 处理 HDFS 路径
     *
     * @param path
     * @return
     */
    public static Pair<String, String> parseHdfsUri(String path) {
        Matcher matcher = pattern.matcher(path);
        if (matcher.find() && matcher.groupCount() == 2) {
            String hdfsUri = matcher.group(1);
            String hdfsPath = matcher.group(2);
            return new MutablePair(hdfsUri, hdfsPath);
        } else {
            return null;
        }
    }

    /**
     * 获取文件或者文件夹的状态
     *
     * @param fs
     * @param remotePath
     * @return
     */
    public static List<FileStatus> listStatus(FileSystem fs, String remotePath) throws IOException {
        log.info("List the status of a folder or file {}", remotePath);
        Path parentPath = new Path(remotePath);
        return Arrays.asList(fs.listStatus(parentPath));
    }

    /**
     * 列出文件
     *
     * @param fs
     * @param remotePath
     * @param isIterate
     * @return
     */
    public static List<FileStatus> listFiles(FileSystem fs, String remotePath, boolean isIterate) throws IOException {
        log.info("List HDFS {} file", remotePath);
        Path parentPath = new Path(remotePath);
        if (!fs.isDirectory(parentPath)) {
            return new ArrayList();
        } else {
            List<FileStatus> allPathList = new ArrayList();
            RemoteIterator locatedFileStatusRemoteIterator = fs.listFiles(parentPath, isIterate);

            while (locatedFileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = (LocatedFileStatus) locatedFileStatusRemoteIterator.next();
                allPathList.add(fileStatus);
            }

            return allPathList;
        }
    }

    /**
     * 列出所有的文件
     *
     * @param fs
     * @param remotePath
     * @return
     */
    public static List<String> listAllFilePath(FileSystem fs, String remotePath) throws IOException {
        log.info("copy HDFS {} file", remotePath);
        Path parentPath = new Path(remotePath);
        if (!fs.isDirectory(parentPath)) {
            return new ArrayList();
        } else {
            List<String> allPathList = new ArrayList();
            RemoteIterator locatedFileStatusRemoteIterator = fs.listFiles(parentPath, true);

            while (locatedFileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = (LocatedFileStatus) locatedFileStatusRemoteIterator.next();
                allPathList.add(fileStatus.getPath().toString());
            }

            return allPathList;
        }
    }

    /**
     * 设置 HDFS 文件权限
     *
     * @param fs
     * @param remotePath
     * @param mode
     */
    public static Boolean setPermission(FileSystem fs, String remotePath, String mode) {
        log.info("setting HDFS {} file permission {}", remotePath, mode);
        try {
            fs.setPermission(new Path(remotePath), new FsPermission(mode));
        } catch (IOException e) {
            throw new DtLoaderException(String.format("setting HDFS file permission failed : %s", e.getMessage()), e);
        }
        return true;
    }

    /**
     * 批量删除 HDFS 文件
     *
     * @param fs
     * @param fileNames
     * @return
     */
    public static boolean deleteFiles(FileSystem fs, List<String> fileNames) {
        log.info("delete HDFS file : {}", fileNames);
        if (CollectionUtils.isEmpty(fileNames)) {
            return true;
        }

        Iterator var3 = fileNames.iterator();
        while (var3.hasNext()) {
            String fileName = (String) var3.next();
            Path path = new Path(fileName);
            try {
                if (fs.exists(path)) {
                    Trash.moveToAppropriateTrash(fs, path, fs.getConf());
                } else {
                    log.error("HDFS file is not exist {}", path);
                }
            } catch (Exception e) {
                throw new DtLoaderException(String.format("judging whether the file exists : %s", e.getMessage()), e);
            }
        }
        return true;
    }

    /**
     * 校验和删除文件
     *
     * @param fs
     * @param remotePath
     * @return
     */
    public static boolean checkAndDelete(FileSystem fs, String remotePath) {
        log.info("delete HDFS file : {}", remotePath);
        remotePath = uri(remotePath);
        Path deletePath = new Path(remotePath);
        try {
            if (fs.exists(deletePath)) {
                return Trash.moveToAppropriateTrash(fs, deletePath, fs.getConf());
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("check or delete file exception : %s", e.getMessage()), e);
        }
        return true;
    }

    /**
     * 获取文件大小
     *
     * @param fs
     * @param remotePath
     * @return
     */
    public static long getDirSize(FileSystem fs, String remotePath) {
        log.info("get HDFS file size : {}", remotePath);
        long size = 0L;

        try {
            if (isDirExist(fs, remotePath)) {
                remotePath = uri(remotePath);
                size = fs.getContentSummary(new Path(remotePath)).getLength();
                log.info(String.format("get dir size:%s", size));
            }
        } catch (Exception var5) {
            log.error(var5.getMessage(), var5);
        }

        return size;
    }

    /**
     * 重命名文件
     *
     * @param fs
     * @param src
     * @param dist
     * @return
     */
    public static boolean rename(FileSystem fs, String src, String dist) {
        log.info("HDFS file rename : {} -> {}", src, dist);
        try {
            return fs.rename(new Path(src), new Path(dist));
        } catch (IOException e) {
            throw new DtLoaderException(String.format("rename hdfs file exception : %s", e.getMessage()), e);
        }
    }

    /**
     * 从路径中获取分区信息
     *
     * @param path
     * @param partitionColumns
     * @return
     */
    public static List<String> parsePartitionDataFromUrl(String path, List<String> partitionColumns) {
        Map<String, String> partColDataMap = new HashMap();
        String[] var3 = path.split("/");
        int var4 = var3.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            String part = var3[var5];
            if (part.contains("=")) {
                String[] parts = part.split("=");
                partColDataMap.put(parts[0], parts[1]);
            }
        }

        List<String> data = new ArrayList();
        Iterator var9 = partitionColumns.iterator();

        while (var9.hasNext()) {
            String partitionColumn = (String) var9.next();
            data.add(partColDataMap.get(partitionColumn));
        }

        return data;
    }
}
