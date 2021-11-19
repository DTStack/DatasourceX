package com.dtstack.dtcenter.common.loader.tdbs.hdfs.downloader;

import com.dtstack.dtcenter.common.loader.tdbs.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.tdbs.hdfs.util.SecurityUtils;
import com.dtstack.dtcenter.common.loader.tdbs.hdfs.YarnConfUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.source.TbdsHdfsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 下载hdfs文件：存储结构为ORC
 *
 * @author ：wangchuan
 * date：Created in 下午01:50 2020/8/11
 * company: www.dtstack.com
 */
public class HdfsORCDownload implements IDownloader {

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
    private TbdsHdfsSourceDTO hdfsSourceDTO;

    private InputSplit[] splits;
    private int splitIndex = 0;

    private InputSplit currentSplit;

    private List<String> partitionColumns;

    private Map<String, Object> kerberosConfig;

    public HdfsORCDownload(TbdsHdfsSourceDTO hdfsSourceDTO, String tableLocation, List<String> columnNames, List<String> partitionColumns, Map<String, Object> kerberosConfig){
        this.hdfsSourceDTO = hdfsSourceDTO;
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        this.kerberosConfig = kerberosConfig;
    }

    @Override
    public boolean configure() throws Exception {

        this.orcSerde = new OrcSerde();
        this.inputFormat = new OrcInputFormat();
        Configuration configuration = YarnConfUtil.getFullConfiguration(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getKerberosConfig());
        conf = new JobConf(configuration);

        Path targetFilePath = new Path(tableLocation);
        FileInputFormat.setInputPaths(conf, targetFilePath);
        splits = inputFormat.getSplits(conf, SPLIT_NUM);
        if(splits !=null && splits.length > 0){
            initRecordReader();
            key = recordReader.createKey();
            value = recordReader.createValue();

            Properties p = new Properties();
            p.setProperty("columns", StringUtils.join(columnNames,","));
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
        return SecurityUtils.login(() -> {
            try {
                return readNextWithKerberos();
            } catch (Exception e){
                throw new DtLoaderException(String.format("Abnormal reading file,%s", e.getMessage()), e);
            }
        }, hdfsSourceDTO.getConfig());
    }

    private List<String> readNextWithKerberos() {
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
    public boolean reachedEnd() {
        return SecurityUtils.login(() -> {
            try {
                return recordReader == null || !nextRecord();
            } catch (Exception e){
                throw new DtLoaderException(String.format("Download file is abnormal,%s", e.getMessage()), e);
            }
        }, hdfsSourceDTO.getConfig());
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
