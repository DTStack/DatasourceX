package com.dtstack.dtcenter.common.loader.hdfs.fileMerge.core;

import com.dtstack.dtcenter.loader.enums.FileFormat;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

@Slf4j
public class TextCombineServer extends CombineServer {

    private int index;

    private String mergedFileName;

    public TextCombineServer() {
    }

    @Override
    protected void doCombine(ArrayList<FileStatus> combineFiles, Path mergedTempPath) throws IOException {
        init();

        //文件大小是128M

        //定义缓冲区大小4096
        byte[] buf = new byte[4096];
        FSDataOutputStream out = null;
        FSDataInputStream in = null;
        try {

            long currentSize = 0L;
            //todo 每次读取判断读取的字节数是否到128M个字节 到了就重新新建一个out流
            for (FileStatus fileStatus : combineFiles) {
                // 打开输入流
                in = fs.open(fileStatus.getPath());
                while (true) {
                    int read = in.read(buf, 0, 4096);
                    if (read == -1) {
                        IOUtils.closeStream(in);
                        break;
                    }
                    currentSize += read;

                    if (out == null) {
                        out = getStream(index++, mergedFileName);
                    }

                    out.write(buf, 0, read);

                    if (currentSize >= maxCombinedFileSize) {
                        IOUtils.closeStream(out);
                        out = null;
                        currentSize = 0;
                    }
                }
            }

        } catch (Exception e) {
            throw new DtLoaderException(String.format("combine file failed,errorMessage :%s ", e.getMessage()), e);
        } finally {
            cleanSource(in, out);
        }

    }

    @Override
    public String getFileSuffix() {
        return FileFormat.TEXT.name();
    }


    @Override
    public long rowSize(FileStatus fileStatus) {
        return 0;
    }


    /**
     * 初始化
     * 合并文件名字前缀
     */
    private void init() {
        this.index = 0;
        this.mergedFileName = System.currentTimeMillis() + "";
    }

    private FSDataOutputStream getStream(int index, String mergedFileName) throws IOException {
        String combineFileName = mergedTempPath.toString() + File.separator
                + mergedFileName + index + "." + getFileSuffix();

        //创建输出流
        return fs.create(new Path(combineFileName));

    }

    private void cleanSource(FSDataInputStream in, FSDataOutputStream out) {
        try {
            if (in != null) {
                IOUtils.closeStream(in);
            }
            if (out != null) {
                IOUtils.closeStream(out);
            }
        } catch (Exception e) {
            log.warn("close inputStream and outStream failed" + e.getMessage());
        }
    }
}
