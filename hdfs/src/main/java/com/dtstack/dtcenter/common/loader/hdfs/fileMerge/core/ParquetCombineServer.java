package com.dtstack.dtcenter.common.loader.hdfs.fileMerge.core;

import com.dtstack.dtcenter.common.loader.hdfs.util.FileUtils;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static java.math.BigDecimal.ROUND_HALF_UP;

@Slf4j
public class ParquetCombineServer extends CombineServer {

    long rowCountSplit;

    private int index;

    private String mergedFileName;

    public ParquetCombineServer() {
    }

    @Override
    protected void doCombine(ArrayList<FileStatus> combineFiles, Path mergedTempPath) throws IOException {
        init(combineFiles);

        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(getParquetSchema(combineFiles.get(0)), configuration);

        List<Path> paths = FileUtils.getPaths(combineFiles);


        ParquetWriter<Group> writer = null;

        long currentCount = 0L;
        try {
            for (Path path : paths) {
                GroupReadSupport readSupport = new GroupReadSupport();
                ParquetReader.Builder<Group> builder = ParquetReader.builder(readSupport, path).withConf(configuration);
                ParquetReader<Group> reader = builder.build();
                Group line;
                while ((line = reader.read()) != null) {
                    currentCount++;
                    if (currentCount >= rowCountSplit) {
                        currentCount = 0L;
                        cleanSource(writer);
                        writer = null;
                    }

                    if (writer == null) {
                        writer = getWriter(index++, groupWriteSupport, mergedFileName);
                    }
                    writer.write(line);
                }
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("combine file failed,errorMessage: %s", e.getMessage()), e);
        } finally {
            cleanSource(writer);
        }
    }

    @Override
    public String getFileSuffix() {
        return FileFormat.PARQUET.name();
    }

    /**
     * 获取文件的数据行数
     *
     * @param fileStatus
     * @return
     * @throws IOException
     */
    @Override
    public long rowSize(FileStatus fileStatus) throws IOException {
        List<Footer> footers = ParquetFileReader.readFooters(configuration, fileStatus, false);
        List<BlockMetaData> blocks = footers.get(0).getParquetMetadata().getBlocks();
        return blocks.get(0).getRowCount();
    }

    /**
     * 获取schema信息
     *
     * @return
     * @throws IOException
     */
    public MessageType getParquetSchema(FileStatus fileStatus) throws IOException {
        ParquetMetadata metaData;
        Path file = fileStatus.getPath();
        MessageType schema;
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(configuration, file)){
            metaData = parquetFileReader.getFooter();
            schema = metaData.getFileMetaData().getSchema();
        }
        return schema;

    }

    /**
     * 初始化
     * 文件写入数量阈值 rowCountSplit
     * 合并文件名字前缀
     *
     * @param combineFiles
     * @throws IOException
     */
    private void init(ArrayList<FileStatus> combineFiles) throws IOException {

        FileStatus fileStatus = combineFiles.get(0);
        long rowSize = rowSize(fileStatus);
        BigDecimal totalSize = new BigDecimal(fileStatus.getLen() + "");
        //1比特对应多少行数据 从而计算出合并后文件对应多少条数据
        BigDecimal divide = new BigDecimal(rowSize + "").divide(totalSize, 4, ROUND_HALF_UP);

        this.index = 0;
        this.rowCountSplit = new BigDecimal(maxCombinedFileSize + "").multiply(divide).longValue();
        this.mergedFileName = System.currentTimeMillis() + "";

    }


    private ParquetWriter getWriter(int id, GroupWriteSupport groupWriteSupport, String mergedFileName) throws IOException {
        String combineFileName = mergedTempPath.toString() + File.separator
                + mergedFileName + id + "." + getFileSuffix();
        return new ParquetWriter<Group>(new Path(combineFileName), configuration, groupWriteSupport);
    }

    private void cleanSource(ParquetWriter<Group> writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                log.warn("close inputStream and outStream failed" + e.getMessage());
            }
        }
    }

}
