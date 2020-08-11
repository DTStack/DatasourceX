package com.dtstack.dtcenter.loader;

import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.HDFSImportColumn;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;

/**
 *
 * 导入数据到hdfs
 *
 * @author ：wangchuan
 * date：Created in 上午10:22 2020/8/11
 * company: www.dtstack.com
 */
public interface IHdfsWriter {

    /**
     * 按位置写入
     * @param source
     * @param hdfsDirPath
     * @param fromLineDelimiter
     * @param toLineDelimiter
     * @param fromFileName
     * @param oriCharSet
     * @param startLine
     * @param topLineIsTitle
     * @param tableColumns
     * @param keyList
     * @return
     */
    int writeByPos(final ISourceDTO source, final String hdfsDirPath, final String fromLineDelimiter, final String toLineDelimiter, final String fromFileName,
                   final String oriCharSet, int startLine, final boolean topLineIsTitle, final List<ColumnMetaDTO> tableColumns, final List<HDFSImportColumn> keyList) throws Exception;

    /**
     * 从文件中读取行,根据提供的分隔符号分割,再根据提供的hdfs分隔符合并,写入hdfs
     * ---需要根据column信息判断导入的数据是否符合要求
     * @param source
     * @param hdfsDirPath
     * @param fromLineDelimiter
     * @param toLineDelimiter
     * @param fromFileName
     * @param oriCharSet
     * @param startLine
     * @param keyList
     * @param tableColumns
     * @return
     * @throws Exception
     */
    int writeByName(final ISourceDTO source, final String hdfsDirPath, final String fromLineDelimiter, final String toLineDelimiter, final String fromFileName,
                    final String oriCharSet, final int startLine, final List<HDFSImportColumn> keyList, final List<ColumnMetaDTO> tableColumns) throws Exception;
}
