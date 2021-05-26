package com.dtstack.dtcenter.common.loader.spark.downloader;

import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import jodd.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * 下载hive表:存储结构为ORC
 * Date: 2020/6/3
 * Company: www.dtstack.com
 * @author wangchuan
 */
@Slf4j
public class SparkORCDownload implements IDownloader {
    private static final int SPLIT_NUM = 1;

    private OrcSerde orcSerde;
    private InputFormat inputFormat;
    private JobConf conf;
    private RecordReader recordReader;

    private int readerCount = 0;
    private Object key;
    private Object value;
    private StructObjectInspector inspector;
    private List<? extends StructField> fields;

    private String tableLocation;
    private List<String> columnNames;
    private Configuration configuration;

    private InputSplit[] splits;
    private int splitIndex = 0;

    private InputSplit currentSplit;

    private List<String> partitionColumns;

    private Map<String, Object> kerberosConfig;

    // 需要查询字段的索引
    private List<Integer> needIndex;

    public SparkORCDownload(Configuration configuration, String tableLocation, List<String> columnNames, List<String> partitionColumns, List<Integer> needIndex, Map<String, Object> kerberosConfig){
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
        this.kerberosConfig = kerberosConfig;
        this.needIndex = needIndex;
    }

    @Override
    public boolean configure() throws Exception {

        this.orcSerde = new OrcSerde();
        this.inputFormat = new OrcInputFormat();
        conf = new JobConf(configuration);

        Path targetFilePath = new Path(tableLocation);
        FileSystem fileSystem = FileSystem.get(configuration);
        // 判断表路径是否存在
        if (!fileSystem.exists(targetFilePath)) {
            log.warn("Table path: {} does not exist", tableLocation);
            return false;
        }
        FileInputFormat.setInputPaths(conf, targetFilePath);
        splits = inputFormat.getSplits(conf, SPLIT_NUM);
        if(splits !=null && splits.length > 0){
            initRecordReader();
            key = recordReader.createKey();
            value = recordReader.createValue();

            Properties p = new Properties();
            p.setProperty("columns", StringUtil.join(columnNames,","));
            orcSerde.initialize(conf, p);

            this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
            fields = inspector.getAllStructFieldRefs();
        }
        return true;
    }

    @Override
    public List<String> getMetaInfo(){
        List<String> metaInfo = new ArrayList<>(columnNames);
        if(CollectionUtils.isNotEmpty(partitionColumns)){
            metaInfo.addAll(partitionColumns);
        }
        return metaInfo;
    }

    @Override
    public List<String> readNext() {
        return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<List<String>>) ()->{
                    try {
                        return readNextWithKerberos();
                    } catch (Exception e){
                        throw new DtLoaderException(String.format("Abnormal reading file,%s", e.getMessage()), e);
                    }
                });
    }

    public List<String> readNextWithKerberos() throws Exception {
        List<String> row = new ArrayList<>();

        // 分区字段的值
        List<String> partitions = Lists.newArrayList();
        if(CollectionUtils.isNotEmpty(partitionColumns)){
            String path = ((OrcSplit)currentSplit).getPath().toString();
            List<String> partData = HdfsOperator.parsePartitionDataFromUrl(path,partitionColumns);
            partitions.addAll(partData);
        }

        // needIndex不为空表示获取指定字段
        if (CollectionUtils.isNotEmpty(needIndex)) {
            for (Integer index : needIndex) {
                // 表示该字段为分区字段
                if (index > columnNames.size() - 1 && CollectionUtils.isNotEmpty(partitions)) {
                    // 分区字段的索引
                    int partIndex = index - columnNames.size();
                    if (partIndex < partitions.size()) {
                        row.add(partitions.get(partIndex));
                    } else {
                        row.add(null);
                    }
                } else if (index < columnNames.size()) {
                    row.add(getFieldByIndex(index));
                } else {
                    row.add(null);
                }
            }
            // needIndex为空表示获取所有字段
        } else {
            for (int index = 0; index < columnNames.size(); index++) {
                row.add(getFieldByIndex(index));
            }
            if (CollectionUtils.isNotEmpty(partitionColumns)) {
                row.addAll(partitions);
            }
        }

        readerCount++;
        return row;
    }

    // 根据index获取字段值
    private String getFieldByIndex(Integer index) {
        if (index > fields.size() -1) {
            return null;
        }
        StructField field = fields.get(index);
        Object data = inspector.getStructFieldData(value, field);
        return Objects.isNull(data) ? null : data.toString();
    }

    private boolean initRecordReader() throws IOException {
        if(splitIndex > splits.length){
            return false;
        }
        OrcSplit orcSplit = (OrcSplit)splits[splitIndex];
        currentSplit = splits[splitIndex];
        splitIndex++;

        if(recordReader != null){
            close();
        }

        recordReader = inputFormat.getRecordReader(orcSplit, conf, Reporter.NULL);
        return true;
    }

    public boolean nextRecord() throws IOException {

        if(recordReader.next(key, value)){
            return true;
        }

        for(int i=splitIndex; i<splits.length; i++){
            initRecordReader();
            if(recordReader.next(key, value)){
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean reachedEnd() {
        return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<Boolean>) ()->{
                    try {
                        return recordReader == null || !nextRecord();
                    } catch (Exception e){
                        throw new DtLoaderException(String.format("Download file is abnormal,%s", e.getMessage()), e);
                    }
                });
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

    @Override
    public List<String> getContainers() {
        return Collections.emptyList();
    }
}
