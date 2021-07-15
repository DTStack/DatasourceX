package com.dtstack.dtcenter.common.loader.kylinRestful.request;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.common.loader.kylinRestful.http.HttpAPI;
import com.dtstack.dtcenter.common.loader.kylinRestful.http.HttpClient;
import com.dtstack.dtcenter.common.loader.kylinRestful.http.HttpClientFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RestfulClient implements Closeable {

    @Override
    public void close() {
    }

    public Boolean auth(KylinRestfulSourceDTO sourceDTO) {
        HttpClient httpClient = HttpClientFactory.createHttpClient(sourceDTO);
        try {
            String result = httpClient.post(HttpAPI.AUTH, null);
            JSONObject.parseObject(result);
            return true;
        } catch (Exception e) {
            log.error("auth error, msg:{}", e.getMessage(), e);
        }
        return false;
    }

    public List<ColumnMetaDTO> getHiveColumnMetaData(KylinRestfulSourceDTO sourceDTO, SqlQueryDTO sqlQueryDTO) {
        String project = sourceDTO.getProject();
        String tableName = sqlQueryDTO.getTableName();
        if (StringUtils.isEmpty(project)) {
            throw new DtLoaderException("get tables exception, project not null");
        }
        if (StringUtils.isEmpty(tableName)) {
            throw new DtLoaderException("get tables exception, tableName not null");
        }
        HttpClient httpClient = HttpClientFactory.createHttpClient(sourceDTO);
        String result = httpClient.get(String.format(HttpAPI.HIVE_A_TABLE, project, tableName), null);
        JSONObject jsonObject = JSONObject.parseObject(result);
        List<ColumnMetaDTO> list = new ArrayList<>();
        if (jsonObject.getString("columns") == null) {
            return list;
        }
        JSONArray jsonArray;
        try {
            jsonArray = JSONArray.parseArray(jsonObject.getString("columns"));
        } catch (Exception e) {
            throw new DtLoaderException("获取元数据异常");
        }

        for (Object object : jsonArray) {
            ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
            JSONObject obj = (JSONObject) object;
            columnMetaDTO.setKey(MapUtils.getString(obj, "name"));
            columnMetaDTO.setType(MapUtils.getString(obj, "datatype"));
        }
        return list;
    }

    /**
     * 获取 hive schema
     *
     * @param sourceDTO
     * @return
     */
    public List<String> getAllHiveDbList(KylinRestfulSourceDTO sourceDTO) {
        String project = sourceDTO.getProject();
        if (StringUtils.isEmpty(project)) {
            throw new DtLoaderException("get tables exception, project not null");
        }

        HttpClient httpClient = HttpClientFactory.createHttpClient(sourceDTO);
        String result = httpClient.get(String.format(HttpAPI.HIVE_LIST_ALL_DB, project), null);
        return JSONArray.parseArray(result, String.class);

    }


    public List<String> getAllHiveTableListBySchema(KylinRestfulSourceDTO sourceDTO, SqlQueryDTO sqlQueryDTO) {
        String project = sourceDTO.getProject();
        String schema = sqlQueryDTO.getSchema();
        if (StringUtils.isEmpty(project)) {
            throw new DtLoaderException("get tables exception, project not null");
        }
        if (StringUtils.isEmpty(schema)) {
            throw new DtLoaderException("get tables exception, schema not null");
        }

        HttpClient httpClient = HttpClientFactory.createHttpClient(sourceDTO);
        String result = httpClient.get(String.format(HttpAPI.HIVE_LIST_ALL_TABLES, schema, project), null);
        return JSONArray.parseArray(result, String.class);
    }


    /**
     * kylin 表
     *
     * @param sourceDTO
     * @param sqlQueryDTO
     * @return
     */
    public List<String> listAllTables(KylinRestfulSourceDTO sourceDTO, SqlQueryDTO sqlQueryDTO) {
        String project = sourceDTO.getProject();
        if (StringUtils.isEmpty(project)) {
            throw new DtLoaderException("get tables exception, project not null");
        }
        HttpClient httpClient = HttpClientFactory.createHttpClient(sourceDTO);
        String result = httpClient.get(HttpAPI.PROJECT_LIST, null);
        JSONArray jsonArray;
        try {
            jsonArray = JSONArray.parseArray(result);
        } catch (Exception e) {
            throw new DtLoaderException("get project list exception", e);
        }
        List<String> list = new ArrayList<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            if (project.equals(jsonObject.getString("name"))) {
                return JSONObject.parseArray(jsonObject.getString("tables"), String.class);
            }
        }
        return list;
    }
}
