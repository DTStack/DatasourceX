package com.dtstack.dtcenter.common.loader.es;

import com.dtstack.dtcenter.common.exception.DtCenterDefException;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:46 2020/2/27
 * @Description：ES 客户端
 */
@Slf4j
public class EsClient extends AbsRdbmsClient {

    private static final int MAX_NUM = 10000;

    @Override
    protected ConnFactory getConnFactory() {
        return null;
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.ES;
    }

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return false;
        }
        return checkConnection(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
    }

    /**
     * 获取es某一索引下所有type（也就是表），6.0版本之后不允许一个index下多个type
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client;
        if (StringUtils.isNotBlank(esSourceDTO.getUsername()) && StringUtils.isNotBlank(esSourceDTO.getPassword())) {
            client = getClient(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
        } else {
            client = getClient(esSourceDTO.getUrl());
        }
        List<String> typeList = Lists.newArrayList();
        //es索引
        String index = queryDTO.getTableName();
        //不指定index抛异常
        if (StringUtils.isBlank(index)){
            throw new DtCenterDefException("未指定es的index，获取失败");
        }
        try {
            GetMappingsResponse res = client.indices().getMapping(new GetMappingsRequest(), RequestOptions.DEFAULT);
            MappingMetaData data = res.mappings().get(index);
            typeList.add(data.type());
        }catch (NullPointerException e){
            log.error("index不存在", e);
            throw new DtCenterDefException("index不存在");
        }catch (Exception e){
            log.error("获取type异常", e);
        }
        return typeList;
    }


    /**
     * 获取es所有索引（也就是数据库）
     * @param iSource
     * @param queryDTO
     * @return
     */
    @Override
    public List<String> getAllDatabases(ISourceDTO iSource, SqlQueryDTO queryDTO){
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client;
        if (StringUtils.isNotBlank(esSourceDTO.getUsername()) && StringUtils.isNotBlank(esSourceDTO.getPassword())) {
            client = getClient(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
        } else {
            client = getClient(esSourceDTO.getUrl());
        }
        ArrayList<String> dbs = new ArrayList<>();
        try {
            GetAliasesRequest aliasesRequest = new GetAliasesRequest();
            GetAliasesResponse alias = client.indices().getAlias(aliasesRequest, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliases = alias.getAliases();
            Set<String> set = aliases.keySet();
            dbs = new ArrayList<>(set);
        }catch (Exception e){
            log.error("获取es索引失败", e);
        }
        return dbs;
    }

    /**
     * es数据预览，默认100条，最大10000条
     * @param iSource
     * @param queryDTO
     * @return
     */
    @Override
    public List<List<Object>> getPreview(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client;
        if (StringUtils.isNotBlank(esSourceDTO.getUsername()) && StringUtils.isNotBlank(esSourceDTO.getPassword())) {
            client = getClient(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
        } else {
            client = getClient(esSourceDTO.getUrl());
        }
        //索引
        String index = queryDTO.getTableName();
        if (StringUtils.isBlank(index)){
            throw new DtCenterDefException("未指定es的index，数据预览失败");
        }
        //限制条数，最大10000条
        int previewNum = queryDTO.getPreviewNum() > MAX_NUM ? MAX_NUM:queryDTO.getPreviewNum();
        //根据index进行查询
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder query = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .size(previewNum);
        SearchRequest source = searchRequest.source(query);
        List<List<Object>> documentList = Lists.newArrayList();
        try {
            SearchResponse search = client.search(source, RequestOptions.DEFAULT);
            //结果集
            SearchHit[] hits = search.getHits().getHits();
            for (SearchHit hit:hits){
                //一行数据
                List<Object> document = Lists.newArrayList();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                sourceAsMap.keySet().forEach(key->
                        document.add(new Pair<String, Object>(key, sourceAsMap.get(key))));
                documentList.add(document);
            }
        }catch (Exception e){
            log.error("获取文档异常", e);
        }
        return documentList;
    }

    /**
     * 获取es字段信息
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client;
        if (StringUtils.isNotBlank(esSourceDTO.getUsername()) && StringUtils.isNotBlank(esSourceDTO.getPassword())) {
            client = getClient(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
        } else {
            client = getClient(esSourceDTO.getUrl());
        }
        //索引
        String index = queryDTO.getTableName();
        if (StringUtils.isBlank(index)){
            throw new DtCenterDefException("未指定es的index,获取字段信息失败");
        }
        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        try {
            //根据index进行查询
            GetMappingsResponse res = client.indices().getMapping(new GetMappingsRequest(), RequestOptions.DEFAULT);
            MappingMetaData data = res.mappings().get(index);
            Map<String, Object> metaDataMap = (Map<String, Object>) data.getSourceAsMap().get("properties");
            Set<String> metaData = metaDataMap.keySet();
            for (String meta:metaData){
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(meta);
                Map<String, Object> map = (Map<String, Object>)metaDataMap.get(meta);
                String type = (String) map.get("type");
                columnMetaDTO.setType(type);
                columnMetaDTOS.add(columnMetaDTO);
            }
        }catch (Exception e){
            log.error("获取文档异常", e);
        }
        return columnMetaDTOS;
    }
    /**
     * 根据连接确定连接成功性
     *
     * @param client
     * @return
     */
    private static boolean checkConnect(RestHighLevelClient client) {
        boolean check = false;
        try {
            client.info(RequestOptions.DEFAULT);
            check = true;
        } catch (Exception e) {
            log.error("", e);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    log.error("", e);
                }
            }
        }
        return check;
    }

    /**
     * 根据地址确认连接性
     *
     * @param address
     * @return
     */
    private static boolean checkConnection(String address, String username, String password) {
        RestHighLevelClient client;
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            client = getClient(address, username, password);
        } else {
            client = getClient(address);
        }

        return checkConnect(client);
    }

    /**
     * 根据用户名密码获取连接
     *
     * @param address
     * @param username
     * @param password
     * @return
     */
    private static RestHighLevelClient getClient(String address, String username, String password) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));
        List<HttpHost> httpHosts = dealHost(address);
        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]))
                        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));
        return client;
    }

    /**
     * 根据地质获取连接
     *
     * @param address
     * @return
     */
    private static RestHighLevelClient getClient(String address) {
        List<HttpHost> httpHosts = dealHost(address);
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
        return client;
    }

    private static List<HttpHost> dealHost(String address) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.valueOf(pair[1]), "http"));
        }
        return httpHostList;
    }

    /******************** 未支持的方法 **********************/
    @Override
    public Connection getCon(ISourceDTO iSource) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}