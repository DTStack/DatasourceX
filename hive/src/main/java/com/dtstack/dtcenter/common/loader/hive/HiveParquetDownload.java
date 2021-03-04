package com.dtstack.dtcenter.common.loader.hive;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.hadoop.GroupTypeIgnoreCase;
import com.dtstack.dtcenter.common.hadoop.HdfsOperator;
import com.dtstack.dtcenter.loader.IDownloader;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 下载hive表:存储结构为PARQUET
 * Date: 2020/6/3
 * Company: www.dtstack.com
 * @author wangchuan
 */
@Slf4j
public class HiveParquetDownload implements IDownloader {

    private int readNum = 0;

    private String tableLocation;

    private List<String> columnNames;

    // 查询字段在所有字段中的索引
    private List<Integer> needIndex;

    private List<String> partitionColumns;

    private Configuration conf;

    private ParquetReader<Group> build;

    private Group currentLine;

    private List<String> paths;

    private JobConf jobConf;

    private int currFileIndex = 0;

    private String currFile;

    private List<String> currentPartData;

    private GroupReadSupport readSupport = new GroupReadSupport();

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private Map<String, Object> kerberosConfig;

    /**
     * 按分区下载
     */
    private Map<String, String> filterPartition;

    public HiveParquetDownload(Configuration conf, String tableLocation, List<String> columnNames,
                               List<String> partitionColumns, List<Integer> needIndex, Map<String, String> filterPartition, Map<String, Object> kerberosConfig){
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        this.conf = conf;
        this.filterPartition = filterPartition;
        this.kerberosConfig = kerberosConfig;
        this.needIndex = needIndex;
    }

    @Override
    public boolean configure() throws Exception {

        jobConf = new JobConf(conf);
        paths = Lists.newArrayList();
        // 递归获取表路径下所有文件
        getAllPartitionPath(tableLocation, paths);
        if(paths.size() == 0){
            throw new RuntimeException("非法路径:" + tableLocation);
        }
        return true;
    }

    private void nextSplitRecordReader() throws Exception{
        if (currFileIndex > paths.size() - 1) {
            return;
        }

        currFile = paths.get(currFileIndex);

        if (!isRequiredPartition()) {
            currFileIndex++;
            nextSplitRecordReader();
        }

        ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(currFile)).withConf(conf);
        build = reader.build();

        if(CollectionUtils.isNotEmpty(partitionColumns)){
            currentPartData = HdfsOperator.parsePartitionDataFromUrl(currFile, partitionColumns);
        }

        currFileIndex++;
    }

    private boolean nextRecord() throws Exception{
        if(build == null && currFileIndex <= paths.size() - 1){
            nextSplitRecordReader();
        }

        if (build == null){
            return false;
        }

        currentLine = build.read();

        if (currentLine == null){
            build = null;
            nextRecord();
        }

        return currentLine != null;
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
    public List<String> readNext() throws Exception {
        return TimeoutExecutor.execAsync(() -> KerberosUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<List<String>>) ()->{
                    try {
                        return readNextWithKerberos();
                    } catch (Exception e){
                        throw new DtCenterDefException("读取文件异常", e);
                    }
                }));
    }

    public List<String> readNextWithKerberos() throws Exception {
        readNum++;

        List<String> line = null;
        if (currentLine != null){
            line = new ArrayList<>();
            // needIndex不为空表示获取指定字段
            if (CollectionUtils.isNotEmpty(needIndex)) {
                for (Integer index : needIndex) {
                    // 表示该字段为分区字段
                    if (index > columnNames.size() - 1 && CollectionUtils.isNotEmpty(currentPartData)) {
                        // 分区字段的索引
                        int partIndex = index - columnNames.size();
                        if (partIndex < currentPartData.size()) {
                            line.add(currentPartData.get(partIndex));
                        } else {
                            line.add(null);
                        }
                    } else if (index < columnNames.size()) {
                        Integer fieldIndex = isFieldExists(columnNames.get(index));
                        if (fieldIndex != -1) {
                            line.add(getFieldByIndex(fieldIndex));
                        }else {
                            line.add(null);
                        }
                    } else {
                        line.add(null);
                    }
                }
                // needIndex为空表示获取所有字段
            } else {
                for (int index = 0; index < columnNames.size(); index++) {
                    Integer fieldIndex = isFieldExists(columnNames.get(index));
                    if (fieldIndex != -1) {
                        line.add(getFieldByIndex(fieldIndex));
                    }else {
                        line.add(null);
                    }
                }
                if(CollectionUtils.isNotEmpty(partitionColumns)){
                    line.addAll(currentPartData);
                }
            }
        }
        return line;
    }

    /**
     * 判断字段是否存在，存在则返回字段索引
     * 每行数据重新判断，对于parquet来说，如果对应schema下没有该列，
     * currentLine.getType().getFields()返回值的size可能会不同，导致数组越界异常!
     * bug 连接：http://redmine.prod.dtstack.cn/issues/33045
     *
     * @param columnName 字段名
     * @return 字段索引
     */
    private Integer isFieldExists (String columnName) {
        GroupTypeIgnoreCase groupType = new GroupTypeIgnoreCase(currentLine.getType());
        if (!groupType.containsField(columnName)) {
            return -1;
        }
        return groupType.getFieldIndex(columnName);
    }

    // 获取指定index下的字段值
    private String getFieldByIndex (Integer index) {
        Type type = currentLine.getType().getType(index);
        String value;
        try{
            // 处理时间戳类型
            if("INT96".equals(type.asPrimitiveType().getPrimitiveTypeName().name())){
                long time = getTimestampMillis(currentLine.getInt96(index,0));
                value = String.valueOf(time);
            } else if (type.getOriginalType() != null && type.getOriginalType().name().equalsIgnoreCase("DATE")){
                value = currentLine.getValueToString(index,0);
                value = new Timestamp(Integer.parseInt(value) * 60 * 60 * 24 * 1000L).toString().substring(0,10);
            } else if (type.getOriginalType() != null && type.getOriginalType().name().equalsIgnoreCase("DECIMAL")){
                DecimalMetadata dm = ((PrimitiveType) type).getDecimalMetadata();
                String primitiveTypeName = currentLine.getType().getType(index).asPrimitiveType().getPrimitiveTypeName().name();
                if ("INT32".equals(primitiveTypeName)){
                    int intVal = currentLine.getInteger(index,0);
                    value = longToDecimalStr((long)intVal,dm.getScale());
                } else if("INT64".equals(primitiveTypeName)){
                    long longVal = currentLine.getLong(index,0);
                    value = longToDecimalStr(longVal,dm.getScale());
                } else {
                    Binary binary = currentLine.getBinary(index,0);
                    value = binaryToDecimalStr(binary,dm.getScale());
                }
            }else {
                value = currentLine.getValueToString(index,0);
            }
        } catch (Exception e){
            value = null;
        }
        return value;
    }

    private static String binaryToDecimalStr(Binary binary,int scale){
        BigInteger bi = new BigInteger(binary.getBytes());
        BigDecimal bg = new BigDecimal(bi,scale);

        return bg.toString();
    }

    private static String longToDecimalStr(long value,int scale){
        BigInteger bi = BigInteger.valueOf(value);
        BigDecimal bg = new BigDecimal(bi, scale);

        return bg.toString();
    }

    /**
     * @param timestampBinary
     * @return
     */
    private long getTimestampMillis(Binary timestampBinary)
    {
        if (timestampBinary.length() != 12) {
            return 0;
        }
        byte[] bytes = timestampBinary.getBytes();

        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return julianDayToMillis(julianDay) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    }

    private long julianDayToMillis(int julianDay)
    {
        return (julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY;
    }

    @Override
    public boolean reachedEnd() throws Exception {
        return TimeoutExecutor.execAsync(() -> KerberosUtil.loginWithUGI(kerberosConfig).doAs(
                (PrivilegedAction<Boolean>) ()->{
                    try {
                        return !nextRecord();
                    } catch (Exception e){
                        throw new DtCenterDefException("下载文件异常", e);
                    }
                }));

    }

    @Override
    public boolean close() throws Exception {
        if (build != null){
            build.close();
            HdfsOperator.release();
        }
        return true;
    }

    @Override
    public String getFileName() {
        return null;
    }

    /**
     * 递归获取文件夹下所有文件，排除隐藏文件和无关文件
     *
     * @param tableLocation hdfs文件路径
     * @param pathList 所有文件集合
     */
    private void getAllPartitionPath(String tableLocation, List<String> pathList) throws IOException {
        Path inputPath = new Path(tableLocation);
        FileSystem fs =  FileSystem.get(jobConf);
        //剔除隐藏系统文件和无关文件
        FileStatus[] fsStatus = fs.listStatus(inputPath, path -> !path.getName().startsWith(".") && !path.getName().startsWith("_SUCCESS") && !path.getName().startsWith("_common_metadata"));
        if(fsStatus == null || fsStatus.length == 0){
            pathList.add(tableLocation);
            return;
        }
        for (FileStatus status : fsStatus) {
            if (status.isFile()) {
                pathList.add(status.getPath().toString());
            }else {
                getAllPartitionPath(status.getPath().toString(), pathList);
            }
        }
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
