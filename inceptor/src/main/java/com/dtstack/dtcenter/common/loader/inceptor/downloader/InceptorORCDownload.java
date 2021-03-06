package com.dtstack.dtcenter.common.loader.inceptor.downloader;

import com.alibaba.fastjson.JSON;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import jodd.util.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class InceptorORCDownload implements IDownloader {
    private static final int SPLIT_NUM = 1;

    private OrcSerde orcSerde;
    private InputFormat inputFormat;
    private JobConf conf;
    private RecordReader recordReader;

    private Object key;
    private Object value;
    private StructObjectInspector inspector;
    private List<? extends StructField> fields;

    private final String tableLocation;
    private final List<String> columnNames;
    private final Configuration configuration;

    private InputSplit[] splits;
    private int splitIndex = 0;

    private InputSplit currentSplit;

    private final List<String> partitionColumns;

    private final Map<String, Object> kerberosConfig;

    /**
     * ???????????????????????????
     */
    private final List<Integer> needIndex;

    /**
     * ????????????
     */
    private final List<String> partitions;

    public InceptorORCDownload(Configuration configuration, String tableLocation, List<String> columnNames,
                           List<String> partitionColumns, List<Integer> needIndex,
                           List<String> partitions, Map<String, Object> kerberosConfig){
        this.configuration = configuration;
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        this.needIndex = needIndex;
        this.partitions = partitions;
        this.kerberosConfig = kerberosConfig;
    }

    @Override
    public boolean configure() throws Exception {

        this.orcSerde = new OrcSerde();
        this.inputFormat = new OrcInputFormat();
        conf = new JobConf(configuration);

        Path targetFilePath = new Path(tableLocation);
        FileSystem fileSystem = FileSystem.get(configuration);
        // ???????????????????????????
        if (!fileSystem.exists(targetFilePath)) {
            log.warn("Table path: {} does not exist", tableLocation);
            return false;
        }
        FileInputFormat.setInputPaths(conf, targetFilePath);
        splits = inputFormat.getSplits(conf, SPLIT_NUM);
        if(ArrayUtils.isNotEmpty(splits)){
            boolean isInit = initRecordReader();
            if (isInit) {
                key = recordReader.createKey();
                value = recordReader.createValue();
                Properties p = new Properties();
                p.setProperty("columns", StringUtil.join(columnNames,","));
                orcSerde.initialize(conf, p);
                this.inspector = (StructObjectInspector) orcSerde.getObjectInspector();
                fields = inspector.getAllStructFieldRefs();
            }
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

    public List<String> readNextWithKerberos() {
        List<String> row = new ArrayList<>();

        // ??????????????????
        List<String> partitions = Lists.newArrayList();
        if(CollectionUtils.isNotEmpty(partitionColumns)){
            String path = ((OrcSplit)currentSplit).getPath().toString();
            List<String> partData = HdfsOperator.parsePartitionDataFromUrl(path,partitionColumns);
            partitions.addAll(partData);
        }

        // needIndex?????????????????????????????????
        if (CollectionUtils.isNotEmpty(needIndex)) {
            for (Integer index : needIndex) {
                // ??????????????????????????????
                if (index > columnNames.size() - 1 && CollectionUtils.isNotEmpty(partitions)) {
                    // ?????????????????????
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
            // needIndex??????????????????????????????
        } else {
            for (int index = 0; index < columnNames.size(); index++) {
                row.add(getFieldByIndex(index));
            }
            if(CollectionUtils.isNotEmpty(partitionColumns)){
                row.addAll(partitions);
            }
        }

        return row;
    }

    // ??????index???????????????
    private String getFieldByIndex(Integer index) {
        if (index > fields.size() -1) {
            return null;
        }
        StructField field = fields.get(index);
        Object data = inspector.getStructFieldData(value, field);
        // ?????? Map ??????
        if (data instanceof Map) {
            return convertMap((Map) data);
        }
        return Objects.isNull(data) ? null : data.toString();
    }

    /**
     * ?????? Map ????????????
     *
     * @param data ??????
     * @return ???????????? String
     */
    private String convertMap(Map data) {
        Map<String, Object> result = new HashMap<>();
        data.keySet().stream().forEach(key -> {
            Object value = data.get(key);
            result.put(key.toString(), Objects.isNull(value) ? null : value.toString());
        });
        return JSON.toJSONString(result);
    }

    private boolean initRecordReader() throws IOException {
        if(splitIndex > splits.length - 1){
            return false;
        }
        OrcSplit orcSplit = (OrcSplit)splits[splitIndex];
        currentSplit = splits[splitIndex];
        splitIndex++;

        if(recordReader != null){
            close();
        }

        // ????????????????????????????????????????????? recordReader orcSplit.getPath().toString() ????????????????????????????????? hdfs ????????????
        if (!isPartitionExists(orcSplit.getPath().toString())) {
            return initRecordReader();
        }

        recordReader = inputFormat.getRecordReader(orcSplit, conf, Reporter.NULL);
        return true;
    }

    public boolean nextRecord() throws IOException {
        if(recordReader.next(key, value)){
            return true;
        }
        for (int i = splitIndex; i < splits.length; i++) {
            if (initRecordReader() && recordReader.next(key, value)) {
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

    /**
     * ????????????????????????
     *
     * @param path hdfs ????????????
     * @return ??????????????????
     */
    private boolean isPartitionExists(String path) {
        // ?????? partitions ??? null?????????????????????????????? true
        if (Objects.isNull(partitions)) {
            return true;
        }
        // ??????????????????????????????????????????????????????????????? false
        if (CollectionUtils.isEmpty(partitions)) {
            return false;
        }
        String curPathPartition = getCurPathPartition(path);
        if (StringUtils.isBlank(curPathPartition)) {
            return false;
        }
        return partitions.contains(curPathPartition);
    }

    /**
     * ?????????????????????????????????
     *
     * @return ??????
     */
    private String getCurPathPartition(String path) {
        StringBuilder curPart = new StringBuilder();
        for (String part : path.split("/")) {
            if(part.contains("=")){
                curPart.append(part).append("/");
            }
        }
        String curPartString = curPart.toString();
        if (StringUtils.isNotBlank(curPartString)) {
            return curPartString.substring(0, curPartString.length() - 1);
        }
        return curPartString;
    }
}