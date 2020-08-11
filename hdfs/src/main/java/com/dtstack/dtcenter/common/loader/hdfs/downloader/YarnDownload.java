package com.dtstack.dtcenter.common.loader.hdfs.downloader;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.HadoopConfTool;
import com.dtstack.dtcenter.common.kerberos.KerberosConfigVerify;
import com.dtstack.dtcenter.common.loader.hdfs.util.HadoopConfUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.LogAggregationUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:53 2020/8/7
 * @Description：Yarn 日志下载
 */
@Slf4j
public class YarnDownload implements IDownloader {
    private static final int bufferSize = 4095;

    //限制字节数
    private int readLimit = bufferSize;

    private Configuration configuration;

    private YarnConfiguration yarnConfiguration;

    private RemoteIterator<FileStatus> nodeFiles;

    private Map<String, Object> yarnConf;

    private String hdfsConfig;

    private static Configuration defaultConfiguration = new Configuration(false);

    private String appIdStr;

    private boolean isReachedEnd = false;

    private FileStatus currFileStatus;

    private DataInputStream currValueStream;

    private String currFileType = "";

    private long currFileLength = 0;

    private String logPreInfo = null;

    private String logEndInfo = null;

    private String currLineValue = "";

    private Integer totalReadByte = 0;

    private byte[] buf = new byte[bufferSize];

    private long curRead = 0L;

    private String logType = null;

    private AggregatedLogFormat.LogKey currLogKey;

    private AggregatedLogFormat.LogReader currReader;

    private static final String FS_HDFS_IMPL_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED = "ipc.client.fallback-to-simple-auth-allowed";

    private YarnDownload(String hdfsConfig, Map<String, Object> yarnConf, String appIdStr, Integer readLimit) {
        this.appIdStr = appIdStr;
        this.yarnConf = yarnConf;
        this.hdfsConfig = hdfsConfig;

        if (readLimit == null || readLimit < bufferSize) {
            log.warn("it is not available readLimit set,it must bigger then " + bufferSize + ", and use default :" + bufferSize);
        } else {
            this.readLimit = readLimit;
        }
    }

    public YarnDownload(String hdfsConfig, Map<String, Object> yarnConf, String appIdStr, Integer readLimit, String logType) {
        this(hdfsConfig, yarnConf, appIdStr, readLimit);
        this.logType = logType;
    }

    @Override
    public void configure() throws Exception {
        configuration = HadoopConfUtil.getFullConfiguration(hdfsConfig, yarnConf);

        //TODO 暂时在这个地方加上
        configuration.set("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");

        yarnConfiguration = new YarnConfiguration(configuration);

        Path remoteRootLogDir = new Path(configuration.get(
                YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
                YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR));

        String logDirSuffix = LogAggregationUtils.getRemoteNodeLogDirSuffix(configuration);
        // TODO Change this to get a list of files from the LAS.

        ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);

        //kerberos认证User
        String jobOwner = UserGroupInformation.getCurrentUser().getShortUserName();
        log.info("applicationId:{},jobOwner:{}", appId, jobOwner);
        Path remoteAppLogDir = LogAggregationUtils.getRemoteAppLogDir(
                remoteRootLogDir, appId, jobOwner, logDirSuffix);
        log.info("applicationId:{},applicationLogPath:{}", appId, remoteAppLogDir.toString());
        checkSize(remoteAppLogDir.toString());
        try {
            Path qualifiedLogDir = FileContext.getFileContext(configuration).makeQualified(remoteAppLogDir);
            nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(), configuration).listStatus(remoteAppLogDir);
            nextLogFile();
        } catch (FileNotFoundException fnf) {
            throw new DtCenterDefException("applicationId:" + appIdStr + " don't have any log file.");
        }
    }

    private void checkSize(String tableLocation) throws IOException {
        Path inputPath = new Path(tableLocation);
        Configuration conf = new JobConf(yarnConfiguration);
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fsStatus = fs.listStatus(inputPath);
        boolean thr = false;
        if (fsStatus == null || fsStatus.length == 0) {
            thr = true;
        } else {
            long totalSize = 0L;
            for (FileStatus file : fsStatus) {
                totalSize += file.getLen();
            }
            if (totalSize == 0L) {
                thr = true;
            }
        }
        if (thr) {
            // 文件大小为0的时候不允许下载，需要重新调用configure接口
            log.error("path:{} size = 0", tableLocation);
            throw new DtCenterDefException("path：" + tableLocation + " size = 0 ");
        }
    }

    @Override
    public List<String> getMetaInfo() throws Exception {
        throw new DtCenterDefException("not support getMetaInfo of App log download");
    }

    @Override
    public Object readNext() throws Exception {
        return currLineValue;
    }

    @Override
    public boolean reachedEnd() throws Exception {
        return isReachedEnd || totalReadByte >= readLimit || !nextRecord();
    }

    @Override
    public void close() throws Exception {
        if(currValueStream != null){
            currValueStream.close();
        }
    }

    @Override
    public String getFileName() {
        return null;
    }

    /**
     * 设置默认 YARN 配置
     *
     * @param conf
     */
    public static void setDefaultConf(Configuration conf) {
        conf.setBoolean(FS_HDFS_IMPL_DISABLE_CACHE, true);
        conf.setBoolean(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED, true);
        conf.set(HadoopConfTool.FS_HDFS_IMPL, HadoopConfTool.DEFAULT_FS_HDFS_IMPL);
    }

    /**
     * 下一个日志文件
     *
     * @return
     * @throws IOException
     */
    private boolean nextLogFile() throws IOException {

        if (currReader != null) {
            currReader.close();
        }

        if (nodeFiles.hasNext()) {
            currFileStatus = nodeFiles.next();
            if (!currFileStatus.getPath().getName()
                    .endsWith(LogAggregationUtils.TMP_FILE_SUFFIX)) {

                currReader = new AggregatedLogFormat.LogReader(configuration, currFileStatus.getPath());
                nextStream();
                try {
                    nextLogType();
                } catch (EOFException e) {
                    //当前logfile已经读取完
                    currLineValue = logEndInfo;
                    logEndInfo = null;
                    if (!nextStream()) {
                        return nextLogFile();
                    }
                    try {
                        nextLogType();
                    } catch (EOFException e1) {
                        if (!nextStream()) {
                            return nextLogFile();
                        }
                        return false;
                    }
                }
                return true;
            } else {
                return nextLogFile();
            }
        } else {
            isReachedEnd = true;
            return false;
        }
    }

    /**
     * 更换下一个container读取
     *
     * @return
     * @throws IOException
     */
    private boolean nextStream() throws IOException {
        currLogKey = new AggregatedLogFormat.LogKey();
        currValueStream = currReader.next(currLogKey);
        return currValueStream != null;
    }

    /**
     * 下一个日志类型
     *
     * @return
     * @throws IOException
     */
    private boolean nextLogType() throws IOException {
        currFileType = currValueStream.readUTF();
        String fileLengthStr = currValueStream.readUTF();
        currFileLength = Long.parseLong(fileLengthStr);

        if (StringUtils.isNotBlank(logType) && !currFileType.toUpperCase().startsWith(logType)) {
            currValueStream.skipBytes(Integer.valueOf(fileLengthStr));
            return nextLogType();
        }

        logPreInfo = "\n\nContainer: " + currLogKey + " on " + currFileStatus.getPath().getName() + "\n";
        logPreInfo = logPreInfo + StringUtils.repeat("=", logPreInfo.length()) + "\n";
        logPreInfo = logPreInfo + "LogType:" + currFileType + "\n";
        logPreInfo = logPreInfo + "LogLength:" + currFileLength + "\n";
        logPreInfo = logPreInfo + "Log Contents:\n";
        curRead = 0L;

        return true;
    }

    private boolean nextRecord() throws IOException {
        if(currValueStream == null && !nextLogFile()){
            isReachedEnd = true;
            currLineValue = null;
            return false;
        }

        if(currFileLength == curRead && logPreInfo != null){
            currLineValue = logPreInfo;
            logPreInfo = null;
            return true;
        }

        //当前logtype已经读取完
        if(currFileLength == curRead){
            logEndInfo = "End of LogType:" + currFileType + "\n";
            try{
                nextLogType();
            }catch (EOFException e){
                //当前logfile已经读取完
                currLineValue = logEndInfo;
                logEndInfo = null;
                if(!nextStream()){
                    return nextLogFile();
                }
                try {
                    nextLogType();
                } catch (EOFException e1) {
                    if(!nextStream()){
                        return nextLogFile();
                    }
                    return false;
                }
                return nextRecord();
            }
        }

        if(currFileLength == 0){
            currLineValue = logEndInfo;
            return true;
        }

        long pendingRead = currFileLength - curRead;
        int toRead = pendingRead > buf.length ? buf.length : (int) pendingRead;

        int readNum = currValueStream.read(buf, 0, toRead);
        curRead += readNum;

        if(readNum <= 0){
            //close stream
            if(currValueStream != null){
                currValueStream.close();
            }

            boolean hasNext = nextLogFile();
            if(!hasNext){
                isReachedEnd = true;
                return false;
            }

            return nextRecord();
        }

        String readLine = new String(buf, 0, readNum);
        totalReadByte += readNum;

        if(logPreInfo != null){
            readLine = logPreInfo + readLine;
            logPreInfo = null;
        }

        if(logEndInfo != null){
            readLine = logEndInfo + readLine;
            logEndInfo = null;
        }

        currLineValue = readLine;
        return true;
    }
}
