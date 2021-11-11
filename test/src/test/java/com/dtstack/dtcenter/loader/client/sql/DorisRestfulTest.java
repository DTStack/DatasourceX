package com.dtstack.dtcenter.loader.client.sql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.dtstack.dtcenter.loader.client.BaseTest;
import com.dtstack.dtcenter.loader.client.ClientCache;
import com.dtstack.dtcenter.loader.client.IClient;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.DorisRestfulSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.math3.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * restful 测试类
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/7
 * company: www.dtstack.com
 */
public class DorisRestfulTest extends BaseTest {

    // 构建client
    private static final IClient CLIENT = ClientCache.getClient(DataSourceType.DorisRestful.getVal());

    // 构建数据源信息
    private static final DorisRestfulSourceDTO SOURCE_DTO = DorisRestfulSourceDTO.builder()
            .url("172.16.21.49:8030")
            .userName("root")
            .schema("qianyi")
            .build();

    /**
     * 连通性测试
     */
    @Test
    public void testCon() {
        Boolean isConnected = CLIENT.testCon(SOURCE_DTO);
        if (Boolean.FALSE.equals(isConnected)) {
            throw new DtLoaderException("connection exception");
        }
    }



    /**
     * 选择schema
     */
    @Test
    public void getTableList() {
        List<String> tableList = CLIENT.getTableListBySchema(SOURCE_DTO, SqlQueryDTO.builder().schema("qianyi").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
        System.out.println(tableList);
    }

    @Test
    public void getAllDatabases() {
        List<String> tableList = CLIENT.getAllDatabases(SOURCE_DTO, SqlQueryDTO.builder().build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
        System.out.println(tableList);
    }

    @Test
    public void getColumnMetaData() {
        List<ColumnMetaDTO> tableList = CLIENT.getColumnMetaData(SOURCE_DTO, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
        System.out.println(tableList);
    }

    @Test
    public void getPreview() {
        List<List<Object>> tableList = CLIENT.getPreview(SOURCE_DTO, SqlQueryDTO.builder().tableName("LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
        System.out.println(tableList);
    }

    @Test
    public void executeQuery() {
        List<Map<String, Object>> tableList = CLIENT.executeQuery(SOURCE_DTO, SqlQueryDTO.builder().sql("select * from LOADER_TEST").build());
        Assert.assertTrue(CollectionUtils.isNotEmpty(tableList));
        System.out.println(tableList);
    }


    private static final String DATA_JSON_PATH = "$.data.data";

    private static final String META_JSON_PATH = "$.data.meta";

    @Test
    public void gg() {
        String json = "{\n" +
                "    \"msg\":\"success\",\n" +
                "    \"code\":0,\n" +
                "    \"data\":{\n" +
                "        \"data\":[\n" +
                "            [\n" +
                "                1,\n" +
                "                \"LOADER_TEST\"\n" +
                "            ]\n" +
                "        ],\n" +
                "        \"meta\":[\n" +
                "            {\n" +
                "                \"name\":\"id\",\n" +
                "                \"type\":\"INT\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"name\":\"name\",\n" +
                "                \"type\":\"CHAR\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"time\":40,\n" +
                "        \"type\":\"result_set\"\n" +
                "    },\n" +
                "    \"count\":0\n" +
                "}";
        JSONArray data = (JSONArray) JSONPath.eval(JSONObject.parse(json), DATA_JSON_PATH);
        JSONArray metaObj = (JSONArray) JSONPath.eval(JSONObject.parse(json), META_JSON_PATH);
        List<String> meta = metaObj.stream().map(obj -> ((JSONObject) obj).getString("name")).collect(Collectors.toList());
        List<List<Object>> resultList = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            List<Object> line = new ArrayList<>();
            JSONArray jsonObject = data.getJSONArray(i);
            for (int j = 0; j < meta.size(); j++)
                line.add(new Pair<String, Object>(meta.get(j), jsonObject.get(j)));
            resultList.add(line);
        }
        System.out.println(resultList);
    }



}
