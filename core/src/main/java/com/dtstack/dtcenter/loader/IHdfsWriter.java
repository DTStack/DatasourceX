package com.dtstack.dtcenter.loader;

import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

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
     * @param hdfsWriterDTO
     * @return
     */
    int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception;

    /**
     * 从文件中读取行,根据提供的分隔符号分割,再根据提供的hdfs分隔符合并,写入hdfs
     * ---需要根据column信息判断导入的数据是否符合要求
     * @param source
     * @param hdfsWriterDTO
     * @return
     * @throws Exception
     */
    int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO) throws Exception;
}
