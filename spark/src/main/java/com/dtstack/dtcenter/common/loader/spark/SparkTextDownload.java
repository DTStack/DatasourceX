package com.dtstack.dtcenter.common.loader.spark;

import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 下载hive表:存储结构为Text
 * Date: 2020/6/3
 * Company: www.dtstack.com
 * @author wangchuan
 */
public class SparkTextDownload implements IDownloader {
    private static final int SPLIT_NUM = 1;

    private static final String IMPALA_INSERT_STAGING = "_impala_insert_staging";

    private TextInputFormat inputFormat;
    private JobConf conf;
    private LongWritable key;
    private Text value;

    private int readNum = 0;

    private RecordReader recordReader;
    private String tableLocation;
    private String fieldDelimiter;
    private Configuration configuration;
    private List<String> columnNames;

    private List<String> paths;
    private String currFile;
    private int currFileIndex = 0;

    private InputSplit[] splits;
    private int splitIndex = 0;
    private List<String> partitionColumns;
    private Map<String, Object> kerberosConfig;
    /**
     * 按分区下载
     */
    private Map<String, String> filterPartition;

    /**
     * 当前分区的值
     */
    private List<String> currentPartData;

    public SparkTextDownload(Configuration configuration, String tableLocation, List<String> columnNames, String fieldDelimiter,
                            List<String> partitionColumns, Map<String, String> filterPartition, Map<String, Object> kerberosConfig){
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.fieldDelimiter = fieldDelimiter;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
        this.filterPartition = filterPartition;
        this.kerberosConfig = kerberosConfig;
    }

    @Override
    public boolean configure() throws IOException {

        paths = getAllPartitionPath(tableLocation);
        if(paths.size() == 0){
            throw new RuntimeException("非法路径:" + tableLocation);
        }

        nextRecordReader();
        key = new LongWritable();
        value = new Text();
        return true;
    }

    private List<String> getAllPartitionPath(String tableLocation) throws IOException {

        Path inputPath = new Path(tableLocation);
        conf = new JobConf(configuration);
        FileSystem fs =  FileSystem.get(conf);

        List<String> pathList = Lists.newArrayList();
        //剔除隐藏系统文件
        FileStatus[] fsStatus = fs.listStatus(inputPath, path -> !path.getName().startsWith(".") && !IMPALA_INSERT_STAGING.equals(path.getName()));

        if(fsStatus == null || fsStatus.length == 0){
            pathList.add(tableLocation);
            return pathList;
        }

        if(fsStatus[0].isDirectory()){
            for(FileStatus status : fsStatus){
                pathList.addAll(getAllPartitionPath(status.getPath().toString()));
            }
            return pathList;
        }else{
            pathList.add(tableLocation);
            return pathList;
        }
    }

    private boolean nextRecordReader() throws IOException {

        if(!nextFile()){
            return false;
        }

        Path inputPath = new Path(currFile);
        conf = new JobConf(configuration);
        inputFormat = new TextInputFormat();

        FileInputFormat.setInputPaths(conf, inputPath);
        TextInputFormat inputFormat = new TextInputFormat();
        inputFormat.configure(conf);
        splits = inputFormat.getSplits(conf, SPLIT_NUM);
        if(splits.length == 0){
            return nextRecordReader();
        }

        if(splits != null && splits.length > 0){
            nextSplitRecordReader();
        }
        return true;
    }

    private boolean nextSplitRecordReader() throws IOException {
        if(splitIndex >= splits.length){
            return false;
        }

        InputSplit fileSplit = splits[splitIndex];
        splitIndex++;

        if(recordReader != null){
            close();
        }
        recordReader = inputFormat.getRecordReader(fileSplit, conf, Reporter.NULL);
        return true;
    }

    private boolean nextFile(){
        if(currFileIndex > (paths.size() - 1)){
            return false;
        }

        currFile = paths.get(currFileIndex);

        if(CollectionUtils.isNotEmpty(partitionColumns)){
            currentPartData = HdfsOperator.parsePartitionDataFromUrl(currFile,partitionColumns);
        }

        if (!isRequiredPartition()){
            currFileIndex++;
            splitIndex = 0;
            nextFile();
        }

        currFileIndex++;
        splitIndex = 0;
        return true;
    }

    public boolean nextRecord() throws IOException {

        if(recordReader.next(key, value)){
            return true;
        }

        //同一个文件夹下是否还存在剩余的split
        while(nextSplitRecordReader()){
            if(nextRecord()){
                return true;
            }
        }

        //查找下一个可读的文件夹
        while (nextRecordReader()){
            if(nextRecord()){
                return true;
            }
        }

        return false;
    }

    @Override
    public List<String> getMetaInfo() throws Exception {
        List<String> metaInfo = new ArrayList<>(columnNames);
        if(CollectionUtils.isNotEmpty(partitionColumns)){
            metaInfo.addAll(partitionColumns);
        }
        return metaInfo;
    }


    @Override
    public List<String> readNext(){
        return TimeoutExecutor.execAsync(() -> KerberosUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<List<String>>) ()->{
                    try {
                        return readNextWithKerberos();
                    } catch (Exception e){
                        throw new DtLoaderException("读取文件异常", e);
                    }
                }));
    }

    public List<String> readNextWithKerberos(){
        readNum++;
        String line = value.toString();
        value.clear();
        String[] fields = line.split(fieldDelimiter);

        List<String> row = new ArrayList<>();

        for (int i = 0; i < columnNames.size(); i++) {
            if(i > fields.length - 1){
                row.add("");
            } else {
                row.add(fields[i]);
            }
        }

        if(CollectionUtils.isNotEmpty(partitionColumns)){
            row.addAll(currentPartData);
        }

        return row;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return TimeoutExecutor.execAsync(() -> KerberosUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<Boolean>) ()->{
                    try {
                        return recordReader == null || !nextRecord();
                    } catch (Exception e){
                        throw new DtLoaderException("下载文件异常", e);
                    }
                }));
    }

    @Override
    public boolean close() throws IOException {
        if(recordReader != null){
            recordReader.close();
        }
        return true;
    }

    @Override
    public String getFileName() {
        return null;
    }

    /**
     * 判断是否是指定的分区，支持多级分区
     * @return
     */
    private boolean isRequiredPartition(){
        if (filterPartition != null && !filterPartition.isEmpty()) {
            //获取当前路径下的分区信息
            Map<String,String> partColDataMap = new HashMap<>();
            for (String part : currFile.split("/")) {
                if(part.contains("=")){
                    String[] parts = part.split("=");
                    partColDataMap.put(parts[0],parts[1]);
                }
            }

            Set<String> keySet = filterPartition.keySet();
            boolean check = true;
            for (String key : keySet) {
                String partition = partColDataMap.get(key);
                String needPartition = filterPartition.get(key);
                if (!Objects.equals(partition, needPartition)){
                    check = false;
                    break;
                }
            }
            return check;
        }
        return true;
    }

    @Override
    public List<String> getContainers() {
        return null;
    }
}
