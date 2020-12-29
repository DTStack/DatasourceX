package com.dtstack.dtcenter.common.loader.hdfs.fileMerge.core;

import com.dtstack.dtcenter.common.loader.hdfs.fileMerge.ECompressType;
import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;

import static java.math.BigDecimal.ROUND_HALF_UP;

@Slf4j
public class OrcCombineServer extends CombineServer {

    //每个合并的新文件可以写的最多数据行数阈值，到达阈值后就进行切割
    private long rowCountSplit;

    //文件编号
    private int index;

    private String mergedFileName;

    public OrcCombineServer() {
    }

    @Override
    protected void doCombine(ArrayList<FileStatus> combineFiles, Path mergedTempPath) throws IOException {
        init(combineFiles);

        Writer writer = null;
        long currentCount = 0L;

        try {
            for (FileStatus fileStatus : combineFiles) {
                Reader reader = getReader(fileStatus);

                RecordReader rows = reader.rows();
                TypeDescription schema = reader.getSchema();
                VectorizedRowBatch batch = schema.createRowBatch();

                while (rows.nextBatch(batch)) {
                    if (writer == null) {
                        writer = getWriter(index++, schema, mergedFileName);
                    }
                    currentCount += batch.size;
                    if (batch.size != 0) {
                        writer.addRowBatch(batch);
                        batch.reset();
                    }
                    //如果达到了写入数量阈值 此文件就不会再进行写入
                    if (currentCount >= rowCountSplit) {
                        currentCount = 0L;
                        writer.close();
                        writer = null;
                    }
                }
                rows.close();
            }
        } catch (Exception e) {
            throw new DtLoaderException(String.format("combine file failed,errorMessage :%s", e.getMessage()), e);
        } finally {
            cleanPackage(writer);
        }
    }


    @Override
    public String getFileSuffix() {
        return FileFormat.ORC.name();
    }


    @Override
    public long rowSize(FileStatus fileStatus) throws IOException {
        return getReader(fileStatus).getNumberOfRows();

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
        Reader reader = getReader(fileStatus);
        long rowSize = rowSize(fileStatus);
        BigDecimal sourceSize = new BigDecimal(fileStatus.getLen() + "")
                .divide(BigDecimal.valueOf(ECompressType.getByTypeAndFileType(reader.getCompressionKind().name(), "orc")
                        .getDeviation()), 4, RoundingMode.HALF_UP);
        BigDecimal divide = new BigDecimal(rowSize + "").divide(sourceSize, 4, ROUND_HALF_UP);


        this.index = 0;
        this.rowCountSplit = new BigDecimal(maxCombinedFileSize + "").multiply(divide).longValue();
        this.mergedFileName = System.currentTimeMillis() + "";
    }


    private Writer getWriter(int index, TypeDescription typeDescription, String radonPath) throws IOException {

        String combineFileName = mergedTempPath.toString() + File.separator
                + radonPath + index + "." + getFileSuffix();
        return OrcFile.createWriter(new Path(combineFileName),
                OrcFile.writerOptions(configuration)
                        //Writer 操作单元，stripe 内容先写入内存，内存满了之后Flush到磁盘
                        .stripeSize(64L * 1024 * 1024)
                        .setSchema(typeDescription));
    }


    private Reader getReader(FileStatus fileStatus) throws IOException {
        return OrcFile.createReader(fileStatus.getPath(),
                OrcFile.readerOptions(configuration));
    }

    private void cleanPackage(Writer writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                log.warn("close inputStream and outStream failed" + e.getMessage());
            }
        }
    }

}
