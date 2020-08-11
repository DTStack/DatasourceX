package com.dtstack.dtcenter.common.loader.spark;

import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.loader.IDownloader;
import jodd.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 下载hive表:存储结构为ORC
 * Date: 2020/6/3
 * Company: www.dtstack.com
 * @author wangchuan
 */

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

    public SparkORCDownload(Configuration configuration, String tableLocation, List<String> columnNames, List<String> partitionColumns){
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        this.configuration = configuration;
    }

    @Override
    public void configure() throws Exception {

        this.orcSerde = new OrcSerde();
        this.inputFormat = new OrcInputFormat();
        conf = new JobConf(configuration);

        Path targetFilePath = new Path(tableLocation);
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
    public List<String> readNext() throws Exception {
        List<String> row = new ArrayList<>();

        int fieldsSize = fields.size();
        for(int i=0; i<fieldsSize; i++){
            StructField field = fields.get(i);
            Object data = inspector.getStructFieldData(value, field);
            if(data == null){
                data = "";
            }

            row.add(data.toString());
        }

        if(CollectionUtils.isNotEmpty(partitionColumns)){
            String path = ((OrcSplit)currentSplit).getPath().toString();
            List<String> partData = HdfsOperator.parsePartitionDataFromUrl(path,partitionColumns);
            row.addAll(partData);
        }

        readerCount++;
        return row;
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
    public boolean reachedEnd() throws IOException {
        return recordReader == null || !nextRecord();
    }

    @Override
    public void close() throws IOException {
        if(recordReader != null){
            recordReader.close();
        }
    }

    @Override
    public String getFileName() {
        return null;
    }
}
