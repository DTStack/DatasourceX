package com.dtstack.dtcenter.common.loader.hdfs.hdfswriter;

import com.csvreader.CsvReader;
import com.dtstack.dtcenter.common.loader.hdfs.util.HadoopConfUtil;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.HDFSImportColumn;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.google.common.collect.Lists;
import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.List;
import java.util.UUID;

/**
 * 文本文件写入hdfs
 *
 * @author ：wangchuan
 * date：Created in 下午01:40 2020/8/11
 * company: www.dtstack.com
 */

public class HdfsTextWriter {

    private static final Logger logger = LoggerFactory.getLogger(HdfsTextWriter.class);

    /**
     * 换行符
     */
    private static final int NEWLINE = 10;

    /**
     * 从文件中读取行,根据提供的分隔符号分割,再根据提供的hdfs分隔符合并,写入hdfs
     * ---需要根据column信息判断导入的数据是否符合要求
     *
     * @param source
     * @param hdfsWriterDTO
     * @throws IOException
     */
    public static int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws IOException {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        int startLine = hdfsWriterDTO.getStartLine();
        if (hdfsWriterDTO.getTopLineIsTitle()) {//首行是标题则内容从下一行开始
            startLine++;
        }

        //FIXME 暂时根据uuid生成文件名称--需要改成和hive原始的名称eg: part-0000
        final String hdfsPath = hdfsWriterDTO.getHdfsDirPath() + "/" + UUID.randomUUID();
        final boolean overwrite = false;

        final Configuration conf = HadoopConfUtil.getHdfsConfiguration(hdfsSourceDTO.getConfig());
        final FileSystem fs = FileSystem.get(conf);
        final Path p = new Path(hdfsPath);
        final OutputStream stream = fs.create(p, overwrite);

        int writeLineNum = 0;
        int currLineNum = 0;

        InputStreamReader inputStreamReader = null;
        CsvReader reader = null;
        try {
            inputStreamReader = HdfsWriter.getReader(hdfsWriterDTO.getFromFileName(), hdfsWriterDTO.getOriCharSet());
            reader = new CsvReader(inputStreamReader, HdfsWriter.getDelim(hdfsWriterDTO.getFromLineDelimiter()));
            while (reader.readRecord()) {
                currLineNum++;
                if (currLineNum < startLine) {
                    continue;
                }

                final String[] lineArray = reader.getValues();
                final String recordStr = transformColumn(hdfsWriterDTO.getColumnsList(), hdfsWriterDTO.getKeyList(), lineArray, hdfsWriterDTO.getToLineDelimiter());

                final byte[] bytes = recordStr.getBytes(Charsets.UTF_8);
                stream.write(bytes);
                stream.write(HdfsTextWriter.NEWLINE);
                stream.flush();
                writeLineNum++;
            }
        } catch (final Exception e) {
            logger.error("", e);
            throw new DtLoaderException("第" + currLineNum + "行数据异常,请检查, 数据导入失败");
        } finally {
            stream.close();

            if (inputStreamReader != null) {
                inputStreamReader.close();
            }

            if (reader != null) {
                reader.close();
            }
        }

        return writeLineNum;
    }

    /**
     * 只有首行为标题行才可以使用名称匹配
     *
     * @param source
     * @param hdfsWriterDTO
     * @param
     * @return
     * @throws IOException
     */
    public static int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws IOException {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) source;
        final boolean overwrite = false;
        final Configuration conf = HadoopConfUtil.getHdfsConfiguration(hdfsSourceDTO.getConfig());
        final FileSystem fs = FileSystem.get(conf);
        final String hdfsPath = hdfsWriterDTO.getHdfsDirPath() + "/" + UUID.randomUUID();
        final Path p = new Path(hdfsPath);
        final OutputStream stream = fs.create(p, overwrite);

        int currLineNum = 0;
        int writeLineNum = 0;
        int startLine = hdfsWriterDTO.getStartLine();
        final List<Integer> indexList = Lists.newArrayList();

        InputStreamReader inputStreamReader = null;
        CsvReader reader = null;
        try {
            inputStreamReader = HdfsWriter.getReader(hdfsWriterDTO.getFromFileName(), hdfsWriterDTO.getOriCharSet());
            reader = new CsvReader(inputStreamReader, HdfsWriter.getDelim(hdfsWriterDTO.getFromLineDelimiter()));
            while (reader.readRecord()) {
                if (currLineNum < (startLine - 1)) {
                    currLineNum++;
                    continue;
                }

                final String[] columnArr = reader.getValues();
                // 首行为标题行
                if (currLineNum == (startLine - 1)) {
                    // 计算出需要使用的索引位置
                    for (final HDFSImportColumn importColum : hdfsWriterDTO.getKeyList()) {
                        if (StringUtils.isBlank(importColum.getKey())) {
                            indexList.add(-1);
                        } else {
                            boolean isMatch = false;
                            for (int i = 0; i < columnArr.length; i++) {
                                final String name = columnArr[i];
                                if (name.equals(importColum.getKey())) {
                                    indexList.add(i);
                                    isMatch = true;
                                    break;
                                }
                            }

                            if (!isMatch) {
                                indexList.add(-1);
                            }
                        }
                    }

                    currLineNum++;
                    continue;
                }

                final StringBuffer sb = new StringBuffer();

                for (int i = 0; i < indexList.size(); i++) {
                    final Integer index = indexList.get(i);
                    if (index == -1) {
                        sb.append(hdfsWriterDTO.getToLineDelimiter());
                    } else if (index > (columnArr.length - 1)) {
                        sb.append(hdfsWriterDTO.getToLineDelimiter());
                    } else {
                        final ColumnMetaDTO columnMeta = hdfsWriterDTO.getColumnsList().get(i);
                        final String targetStr = HdfsWriter.convertToTargetType(columnMeta.getType(), columnArr[index], hdfsWriterDTO.getKeyList().get(i).getDateFormat()).toString();
                        sb.append(targetStr).append(hdfsWriterDTO.getToLineDelimiter());
                    }
                }

                String recordStr = sb.toString();
                if (recordStr.endsWith(hdfsWriterDTO.getToLineDelimiter())) {
                    recordStr = recordStr.substring(0, recordStr.length() - 1);
                }

                final byte[] bytes = recordStr.getBytes(Charsets.UTF_8);
                stream.write(bytes);
                stream.write(NEWLINE);
                stream.flush();
                currLineNum++;
                writeLineNum++;
            }
        } catch (final Exception e) {
            logger.error("", e);
            throw new DtLoaderException("(第" + currLineNum + "行数据异常,请检查。数据导入失败)");
        } finally {
            stream.close();

            if (inputStreamReader != null) {
                inputStreamReader.close();
            }

            if (reader != null) {
                reader.close();
            }
        }
        return writeLineNum;
    }

    private static String transformColumn(final List<ColumnMetaDTO> tableColumns, final List<HDFSImportColumn> keyList, final String[] columnValArr, final String delimiter) throws ParseException {

        if (columnValArr == null) {
            throw new DtLoaderException("记录不应该为空");
        }

        final int length = columnValArr.length > tableColumns.size() ? tableColumns.size() : columnValArr.length;

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            final String columnVal = columnValArr[i];
            final ColumnMetaDTO tableColumn = tableColumns.get(i);
            final String columnType = tableColumn.getType();
            final Object targetVal = HdfsWriter.convertToTargetType(columnType, columnVal, keyList.get(i).getDateFormat());
            if (targetVal != null) {
                sb.append(targetVal);
            }

            if (i != (length - 1)) {
                sb.append(delimiter);
            }
        }

        return sb.toString();
    }
}
