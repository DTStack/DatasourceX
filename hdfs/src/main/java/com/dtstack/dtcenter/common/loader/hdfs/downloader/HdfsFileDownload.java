package com.dtstack.dtcenter.common.loader.hdfs.downloader;

import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.common.loader.hdfs.YarnConfUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author sanyue
 */
public class HdfsFileDownload implements IDownloader {
    private static final Logger logger = LoggerFactory.getLogger(HdfsFileDownload.class);
    private static final int READ_LIMIT = 1000;
    private static final String CRLF = System.lineSeparator();
    private static String jobOwner = System.getenv("HADOOP_USER_NAME");

    private static final int SPLIT_NUM = 1;

    private TextInputFormat inputFormat;
    private JobConf conf;
    private LongWritable key;
    private Text value;

    private int readNum = 0;

    private RecordReader recordReader;

    private String defaultFs;
    private String hdfsConfig;
    private Map<String, Object> kerberosConfig;
    private Map<String, Object> yarnConf;

    private List<String> paths;
    private String currFile;
    private int currFileIndex = 0;

    private InputSplit[] splits;
    private int splitIndex = 0;

    /**
     * new
     */
    private String path;

    private YarnConfiguration yarnConfiguration;

    public HdfsFileDownload(String defaultFs, String hdfsConfig, String path, Map<String, Object> yarnConf, Map<String, Object> kerberosConfig) {
        this.defaultFs = defaultFs;
        this.hdfsConfig = hdfsConfig;
        this.path = path;
        this.yarnConf = yarnConf;
        this.kerberosConfig = kerberosConfig;
    }

    public void configure(String defaultJobOwner) throws IOException {
        jobOwner = jobOwner == null ? defaultJobOwner : jobOwner;
        Configuration configuration = YarnConfUtil.getFullConfiguration(defaultFs, hdfsConfig, yarnConf, kerberosConfig);
        yarnConfiguration = new YarnConfiguration(configuration);
        paths = checkPath(path);
        if (paths.size() == 0) {
            throw new RuntimeException("Illegal path:" + path);
        }
        nextRecordReader();
        key = new LongWritable();
        value = new Text();
    }

    private List<String> checkPath(String tableLocation) throws IOException {

        Path inputPath = new Path(tableLocation);
        conf = new JobConf(yarnConfiguration);
        FileSystem fs = FileSystem.get(conf);

        List<String> pathList = Lists.newArrayList();

        //剔除隐藏系统文件
        FileStatus[] fsStatus = fs.listStatus(inputPath, path -> !path.getName().startsWith("."));

        checkSize(fsStatus, tableLocation);

        if (fsStatus[0].isDirectory()) {
            for (FileStatus status : fsStatus) {
                pathList.addAll(checkPath(status.getPath().toString()));
            }
            return pathList;
        } else {
            pathList.add(tableLocation);
            return pathList;
        }
    }

    private void checkSize(FileStatus[] fsStatus, String tableLocation) {
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
            //文件大小为0的时候不允许下载，需要重新调用configure接口
            throw new DtLoaderException("path：" + tableLocation + " size = 0 ");
        }
    }

    private boolean nextRecordReader() throws IOException {

        if (!nextFile()) {
            return false;
        }

        Path inputPath = new Path(currFile);
        conf = new JobConf(yarnConfiguration);
        inputFormat = new TextInputFormat();

        FileInputFormat.setInputPaths(conf, inputPath);
        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(conf);
        splits = inputFormat.getSplits(conf, SPLIT_NUM);
        if (splits.length == 0) {
            return nextRecordReader();
        }

        if (splits != null && splits.length > 0) {
            nextSplitRecordReader();
        }
        return true;
    }

    private boolean nextSplitRecordReader() throws IOException {
        if (splitIndex >= splits.length) {
            return false;
        }

        InputSplit fileSplit = splits[splitIndex];
        splitIndex++;

        if (recordReader != null) {
            close();
        }

        recordReader = inputFormat.getRecordReader(fileSplit, conf, Reporter.NULL);
        return true;
    }

    private boolean nextFile() {
        if (currFileIndex > (paths.size() - 1)) {
            return false;
        }

        currFile = paths.get(currFileIndex);

        currFileIndex++;
        splitIndex = 0;
        return true;
    }

    public boolean nextRecord() throws IOException {

        if (recordReader.next(key, value)) {
            return true;
        }

        //同一个文件夹下是否还存在剩余的split
        while (nextSplitRecordReader()) {
            if (nextRecord()) {
                return true;
            }
        }

        //查找下一个可读的文件夹
        while (nextRecordReader()) {
            if (nextRecord()) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean configure() throws Exception {
        configure(jobOwner);
        return true;
    }

    @Override
    public List<String> getMetaInfo() {
        return Collections.emptyList();
    }

    @Override
    public String readNext() {
        return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<String>) ()->{
                    try {
                        return readNextWithKerberos();
                    } catch (Exception e){
                        throw new DtLoaderException(String.format("Abnormal reading file,%s", e.getMessage()), e);
                    }
                });

    }

    private String readNextWithKerberos() {
        readNum++;
        try {
            //第一行和最后一行乱码跳过
            if (readNum == 1 || Math.abs(recordReader.getProgress() - 1.0F) < 0.000001) {
                return "";
            }
        } catch (IOException e) {
            logger.error("readNext error", e);
        }
        String line = new String(value.toString());
        line = line + CRLF;
        return line;
    }

    @Override
    public boolean reachedEnd() {
        return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<Boolean>) () -> {
                    try {
                        return recordReader == null || readNum > READ_LIMIT || !nextRecord();
                    } catch (Exception e) {
                        throw new DtLoaderException(String.format("Abnormal reading file,%s", e.getMessage()), e);
                    }
                });
    }

    @Override
    public boolean close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
        return true;
    }

    @Override
    public String getFileName() {
        return null;
    }

    @Override
    public List<String> getContainers() {
        return Collections.emptyList();
    }

}
