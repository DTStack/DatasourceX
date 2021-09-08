package com.dtstack.dtcenter.common.loader.tdbs.hdfs;

import com.dtstack.dtcenter.common.loader.common.DtClassConsistent;
import com.dtstack.dtcenter.common.loader.hadoop.hdfs.HdfsOperator;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.TbdsHdfsSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang3.StringUtils;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:53 2020/2/27
 * @Description：HDFS 连接工厂
 */
public class HdfsConnFactory {

    public Boolean testConn(ISourceDTO iSource) {
        TbdsHdfsSourceDTO hdfsSourceDTO = (TbdsHdfsSourceDTO) iSource;
        if (StringUtils.isBlank(hdfsSourceDTO.getDefaultFS()) || !hdfsSourceDTO.getDefaultFS().matches(DtClassConsistent.HadoopConfConsistent.DEFAULT_FS_REGEX)) {
            throw new DtLoaderException("defaultFS incorrect format");
        }

        return HdfsOperator.checkConnection(hdfsSourceDTO.getDefaultFS(), hdfsSourceDTO.getConfig(), hdfsSourceDTO.getKerberosConfig());
    }
}
