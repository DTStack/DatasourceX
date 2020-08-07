package com.dtstack.dtcenter.common.loader.hdfs;

import com.dtstack.dtcenter.common.hadoop.DtKerberosUtils;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.HdfsSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import org.apache.commons.collections.MapUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 16:53 2020/2/27
 * @Description：hdfs 客户端
 */
public class HdfsClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return new HdfsConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.HDFS;
    }

    @Override
    public IDownloader getDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        HdfsSourceDTO hdfsSourceDTO = (HdfsSourceDTO) iSource;
        // todo 直接认证
        //kerberos认证
        if (MapUtils.isNotEmpty(hdfsSourceDTO.getKerberosConfig())) {
            DtKerberosUtils.loginKerberos(hdfsSourceDTO.getKerberosConfig());
        }

        YarnDownload yarnDownload = new YarnDownload(hdfsSourceDTO.getYarnConf(), hdfsSourceDTO.getAppIdStr(), hdfsSourceDTO.getReadLimit(), hdfsSourceDTO.getLogType());
        yarnDownload.configure();
        return yarnDownload;
    }

    /************************************** 不支持的方法 ****************************************/
    @Override
    public Connection getCon(ISourceDTO iSourceDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableList(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSourceDTO, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}