package com.dtstack.dtcenter.common.loader.spark.downloader;

import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.common.loader.spark.GroupTypeIgnoreCase;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
public class SparkParquetDownload implements IDownloader {

    private int readNum = 0;

    private String tableLocation;

    private List<String> columnNames;

    private List<Integer> columnIndex;

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

    public SparkParquetDownload(Configuration conf, String tableLocation, List<String> columnNames,
                               List<String> partitionColumns, Map<String, String> filterPartition, Map<String, Object> kerberosConfig){
        this.tableLocation = tableLocation;
        this.columnNames = columnNames;
        this.partitionColumns = partitionColumns;
        this.conf = conf;
        this.filterPartition = filterPartition;
        this.kerberosConfig = kerberosConfig;
    }

    @Override
    public boolean configure() throws Exception {


        jobConf = new JobConf(conf);

        paths = getAllPartitionPath(tableLocation);
        if(paths.size() == 0){
            throw new DtLoaderException("非法路径:" + tableLocation);
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
        if (MapUtils.isNotEmpty(kerberosConfig)) {
            build = KerberosUtil.loginKerberosWithUGI(kerberosConfig).doAs(
                    (PrivilegedAction<ParquetReader<Group>>) () -> {
                        try {
                            return reader.build();
                        } catch (IOException e) {
                            throw new DtLoaderException(e.getMessage(), e);
                        }
                    }
            );
        } else {
            build = reader.build();
        }

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
        readNum++;

        List<String> line = null;
        if (currentLine != null){
            line = new ArrayList<>();
            if (columnIndex == null){
                columnIndex = new ArrayList<>();
                for (String columnName : columnNames) {
                    GroupTypeIgnoreCase groupType = new GroupTypeIgnoreCase(currentLine.getType());
                    columnIndex.add(groupType.containsField(columnName) ?
                            groupType.getFieldIndex(columnName) : -1);
                }
            }

            if (CollectionUtils.isNotEmpty(columnIndex)){
                line = new ArrayList<>();
                for (Integer index : columnIndex) {
                    if(index == -1){
                        line.add(null);
                    } else {
                        Type type = currentLine.getType().getType(index);
                        String value = null;

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

                        line.add(value);
                    }
                }
            }
        }

        if(CollectionUtils.isNotEmpty(partitionColumns)){
            line.addAll(currentPartData);
        }

        return line;
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
        return !nextRecord();
    }

    @Override
    public boolean close() throws Exception {
        if (build != null){
            build.close();
        }
        return true;
    }

    @Override
    public String getFileName() {
        return null;
    }

    private List<String> getAllPartitionPath(String tableLocation) throws IOException {
        Path inputPath = new Path(tableLocation);
        FileSystem fs =  FileSystem.get(jobConf);

        List<String> pathList = Lists.newArrayList();
        //剔除隐藏系统文件
        FileStatus[] fsStatus = fs.listStatus(inputPath, path -> !path.getName().startsWith("."));

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
