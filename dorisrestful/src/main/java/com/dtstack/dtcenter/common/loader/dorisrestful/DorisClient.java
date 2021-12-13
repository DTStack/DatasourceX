package com.dtstack.dtcenter.common.loader.dorisrestful;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.dorisrestful.request.DorisRestfulClient;
import com.dtstack.dtcenter.common.loader.dorisrestful.request.DorisRestfulClientFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.DorisRestfulSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.List;
import java.util.Map;

/**
 * doris 客户端
 *
 * @author ：qianyi
 * date：Created in 上午10:33 2021/7/14
 * company: www.dtstack.com
 */
public class DorisClient<T> extends AbsNoSqlClient<T> {


    @Override
    public Boolean testCon(ISourceDTO source) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.login(sourceDTO);
    }


    @Override
    public List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.getAllDatabases(sourceDTO);
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.getTableListBySchema(sourceDTO, queryDTO);
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.getTableListBySchema(sourceDTO, queryDTO);
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.getColumnMetaData(sourceDTO, queryDTO);
    }

    @Override
    public List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.getPreview(sourceDTO, queryDTO);
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.executeQuery(sourceDTO, queryDTO);
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO) {
        DorisRestfulSourceDTO sourceDTO = buildSourceDTO(source);
        DorisRestfulClient restfulClient = DorisRestfulClientFactory.getRestfulClient();
        return restfulClient.executeSqlWithoutResultSet(sourceDTO, queryDTO);
    }


    /**
     * copy 对象
     * @param source
     * @return
     */
    public DorisRestfulSourceDTO buildSourceDTO(ISourceDTO source) {
        DorisRestfulSourceDTO sourceDTO = (DorisRestfulSourceDTO) source;
        try {
            return DorisRestfulSourceDTO.builder()
                    .url(sourceDTO.getUrl())
                    .cluster(sourceDTO.getCluster())
                    .schema(sourceDTO.getSchema())
                    .userName(sourceDTO.getUsername())
                    .password(sourceDTO.getPassword())
                    .build();
        } catch (Exception e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
