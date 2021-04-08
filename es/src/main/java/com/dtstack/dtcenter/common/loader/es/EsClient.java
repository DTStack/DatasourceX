package com.dtstack.dtcenter.common.loader.es;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.dtstack.dtcenter.common.loader.common.exception.IErrorPattern;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.service.ErrorAdapterImpl;
import com.dtstack.dtcenter.common.loader.common.service.IErrorAdapter;
import com.dtstack.dtcenter.common.loader.es.pool.ElasticSearchManager;
import com.dtstack.dtcenter.common.loader.es.pool.ElasticSearchPool;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:46 2020/2/27
 * @Description：ES 客户端
 */
@Slf4j
public class EsClient<T> extends AbsNoSqlClient<T> {

    private static final int MAX_NUM = 10000;

    private static final String POST = "post";

    private static final String PUT = "put";

    private static final String DELETE = "delete";

    private static final String ENDPOINT_SEARCH_FORMAT = "/%s/_search";

    private static final String ENDPOINT_UPDATE_FORMAT = "/%s/_update";

    private static final String ENDPOINT_DELETE_FORMAT = "/%s";

    private static final String ENDPOINT_BULK_FORMAT = "/_bulk";

    private static final String ENDPOINT_UPDATE_QUERY_FORMAT = "/%s/_update_by_query";

    private static final String ENDPOINT_DELETE_QUERY_FORMAT = "/%s/_delete_by_query";

    private static final String RESULT_KEY = "result";

    private static ElasticSearchManager elasticSearchManager = ElasticSearchManager.getInstance();

    public static final ThreadLocal<Boolean> IS_OPEN_POOL = new ThreadLocal<>();

    private static final IErrorPattern ERROR_PATTERN = new EsErrorPattern();

    // 异常适配器
    private static final IErrorAdapter ERROR_ADAPTER = new ErrorAdapterImpl();

    @Override
    public Boolean testCon(ISourceDTO iSource) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return false;
        }
        RestHighLevelClient client = null;
        try {
            client = getClient(esSourceDTO);
            client.info(RequestOptions.DEFAULT);
            return true;
        } catch (Exception e) {
            throw new DtLoaderException(ERROR_ADAPTER.connAdapter(e.getMessage(), ERROR_PATTERN), e);
        } finally {
            closeResource(null, client, esSourceDTO);
        }
    }

    /**
     * 获取es某一索引下所有type（也就是表），6.0版本之后不允许一个index下多个type
     *
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client = getClient(esSourceDTO);
        List<String> typeList = Lists.newArrayList();
        //es索引
        String index = queryDTO.getTableName();
        //不指定index抛异常
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified, and the acquisition fails");
        }
        try {
            GetMappingsRequest request = new GetMappingsRequest();
            // 为了兼容7.x之前的版本，所以把参数设置为null
            request.setMasterTimeout(null);
            request.indicesOptions(null);
            GetMappingsResponse res = client.indices().getMapping(request, RequestOptions.DEFAULT);
            MappingMetaData data = res.mappings().get(index);
            typeList.add(data.type());
        } catch (NullPointerException e) {
            throw new DtLoaderException("index not exits");
        } catch (Exception e) {
            log.error("get type exception", e);
        } finally {
            closeResource(null, client, esSourceDTO);
        }
        return typeList;
    }


    /**
     * 获取es所有索引（也就是数据库）
     *
     * @param iSource
     * @param queryDTO
     * @return
     */
    @Override
    public List<String> getAllDatabases(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client = getClient(esSourceDTO);
        ArrayList<String> dbs = new ArrayList<>();
        try {
            GetAliasesRequest aliasesRequest = new GetAliasesRequest();
            GetAliasesResponse alias = client.indices().getAlias(aliasesRequest, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliases = alias.getAliases();
            Set<String> set = aliases.keySet();
            dbs = new ArrayList<>(set);
        } catch (Exception e) {
            log.error("Failed to get es index", e);
        } finally {
            closeResource(null, client, esSourceDTO);
        }
        return dbs;
    }

    /**
     * es数据预览，默认100条，最大10000条
     *
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
        RestHighLevelClient client = getClient(esSourceDTO);
        //索引
        String index = queryDTO.getTableName();
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified，Data preview failed");
        }
        //限制条数，最大10000条
        int previewNum = queryDTO.getPreviewNum() > MAX_NUM ? MAX_NUM : queryDTO.getPreviewNum();
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
            for (SearchHit hit : hits) {
                //一行数据
                List<Object> document = Lists.newArrayList();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                sourceAsMap.keySet().forEach(key ->
                        document.add(new Pair<String, Object>(key, sourceAsMap.get(key))));
                documentList.add(document);
            }
        } catch (Exception e) {
            log.error("doc acquisition exception", e);
        } finally {
            closeResource(null, client, esSourceDTO);
        }
        return documentList;
    }

    /**
     * 获取es字段信息
     *
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        RestHighLevelClient client = getClient(esSourceDTO);
        //索引
        String index = queryDTO.getTableName();
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified, and the field information fails to be obtained");
        }
        List<ColumnMetaDTO> columnMetaDTOS = new ArrayList<>();
        try {
            //根据index进行查询
            GetMappingsRequest request = new GetMappingsRequest();
            // 为了兼容7.x之前的版本，所以把参数设置为null
            request.setMasterTimeout(null);
            request.indicesOptions(null);
            GetMappingsResponse res = client.indices().getMapping(request, RequestOptions.DEFAULT);
            MappingMetaData data = res.mappings().get(index);
            Map<String, Object> metaDataMap = (Map<String, Object>) data.getSourceAsMap().get("properties");
            Set<String> metaData = metaDataMap.keySet();
            for (String meta : metaData) {
                ColumnMetaDTO columnMetaDTO = new ColumnMetaDTO();
                columnMetaDTO.setKey(meta);
                Map<String, Object> map = (Map<String, Object>) metaDataMap.get(meta);
                String type = (String) map.get("type");
                columnMetaDTO.setType(type);
                columnMetaDTOS.add(columnMetaDTO);
            }
        } catch (Exception e) {
            log.error("doc acquisition exception", e);
        } finally {
            closeResource(null, client, esSourceDTO);
        }
        return columnMetaDTOS;
    }

    @Override
    public List<Map<String, Object>> executeQuery(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        List<Map<String, Object>> list = Lists.newArrayList();
        HashMap<String, Object> map = Maps.newHashMap();
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return new ArrayList<>();
        }
        String dsl = doDealPageSql(queryDTO.getSql());
        //索引
        String index = queryDTO.getTableName();
        if (StringUtils.isBlank(index)) {
            throw new DtLoaderException("The index of es is not specified, and the field information fails to be obtained. Please specify tableName as the index in sqlQueryDTO");
        }
        RestHighLevelClient client = null;
        RestClient lowLevelClient = null;
        JSONObject resultJsonObject = null;
        try (NStringEntity entity = new NStringEntity(dsl, ContentType.APPLICATION_JSON)) {
            client = getClient(esSourceDTO);
            if (Objects.isNull(client)) {
                throw new DtLoaderException("No database connection available");
            }
            lowLevelClient = client.getLowLevelClient();
            Request request = new Request(POST, String.format(ENDPOINT_SEARCH_FORMAT, index));
            request.setEntity(entity);
            Response response = lowLevelClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            resultJsonObject = JSONObject.parseObject(result);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(lowLevelClient, client, esSourceDTO);
        }
        map.put(RESULT_KEY, resultJsonObject);
        list.add(map);
        return list;
    }

    private static RestHighLevelClient getClient(ESSourceDTO esSourceDTO) {
        boolean check = esSourceDTO.getPoolConfig() != null;
        IS_OPEN_POOL.set(check);
        if (!check) {
            return getClient(esSourceDTO.getUrl(), esSourceDTO.getUsername(), esSourceDTO.getPassword());
        }
        ElasticSearchPool elasticSearchPool = elasticSearchManager.getConnection(esSourceDTO);
        RestHighLevelClient restHighLevelClient = elasticSearchPool.getResource();
        if (Objects.isNull(restHighLevelClient)) {
            throw new DtLoaderException("No database connection available");
        }
        return restHighLevelClient;

    }

    /**
     * 1. 根据地址、用户名和密码连接 esClient
     * 2. username或者password为空时，根据地址获取esClient
     *
     * @param address
     * @param username
     * @param password
     * @return
     */
    private static RestHighLevelClient getClient(String address, String username, String password) {
        log.info("Get ES data source connection, address : {}, userName : {}", address, username);
        List<HttpHost> httpHosts = dealHost(address);
        //有用户名密码情况
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));
            return new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)));
        }
        //无用户名密码
        return new RestHighLevelClient(RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()])));
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

    private void closeResource(RestClient lowLevelClient, RestHighLevelClient restHighLevelClient, ESSourceDTO esSourceDTO) {
        if (!BooleanUtils.isTrue(IS_OPEN_POOL.get()) && restHighLevelClient != null) {
            try {
                if (Objects.nonNull(lowLevelClient)) {
                    lowLevelClient.close();
                }
                if (Objects.nonNull(restHighLevelClient)) {
                    restHighLevelClient.close();
                }
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
            IS_OPEN_POOL.remove();
        } else {
            ElasticSearchPool elasticSearchPool = elasticSearchManager.getConnection(esSourceDTO);
            try {
                if (Objects.nonNull(lowLevelClient)) {
                    lowLevelClient.close();
                }
                if (Objects.nonNull(restHighLevelClient) && Objects.nonNull(elasticSearchPool)) {
                    elasticSearchPool.returnResource(restHighLevelClient);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private String doDealPageSql(String dsl) {
        if (StringUtils.isNotBlank(dsl)) {
            // Feature.OrderedField 是为了保证格式化后 json 中的字段顺序不变
            JSONObject jsonObject = JSONObject.parseObject(dsl, Feature.OrderedField);
            return JSONObject.toJSONString(jsonObject, true);
        }
        return "";
    }

    /******************** 未支持的方法 **********************/
    @Override
    public Connection getCon(ISourceDTO iSource) {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO iSource, SqlQueryDTO sqlQueryDTO) {
        throw new DtLoaderException("Not Support");
    }

    /**
     * 执行增删改等操作
     * <p><p/>
     * 默认执行POST请求，请求参数中的tableName作为esclient的endpoint.
     * <p><p/>
     * 如果需要执行其他类型的POST请求，则esCommandType 为0
     *
     * <ul>
     *     <li>DEFAULT(0) insert 操作，插入时要指定_id</li>
     *     <li>INSERT(1) insert 操作，插入时要指定_id</li>
     *     <li>UPDATE(2) _update 操作，指定_id</li>
     *     <li>DELETE(3) delete操作，删除单条数据要指定_id</li>
     *     <li>BULK(4) _bulk批量操作，请求/_bulk,需要在endpoint中指定_index和_type</li>
     *     <li>UPDATE_BY_QUERY(5) _update_by_query 根据条件更新,需要在endpoint中指定_index和_type</li>
     *     <li>DELETE_BY_QUERY(6) _delete_by_query 根据条件删除,需要在endpoint中指定_index和_type</li>
     * <ul/>
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    @Override
    public Boolean executeSqlWithoutResultSet(ISourceDTO iSource, SqlQueryDTO queryDTO) {
        ESSourceDTO esSourceDTO = (ESSourceDTO) iSource;
        Boolean result = false;
        if (esSourceDTO == null || StringUtils.isBlank(esSourceDTO.getUrl())) {
            return null;
        }
        //索引
        String index = queryDTO.getTableName();

        RestHighLevelClient client = null;
        RestClient lowLevelClient = null;
        NStringEntity entity = null;
        try {
            client = getClient(esSourceDTO);
            if (Objects.isNull(client)) {
                throw new DtLoaderException("No database connection available");
            }
            lowLevelClient = client.getLowLevelClient();

            if (queryDTO.getSql() != null) {
                entity = new NStringEntity(queryDTO.getSql(), ContentType.APPLICATION_JSON);
            }
            Integer esCommandType = queryDTO.getEsCommandType();
            String httpMethod = POST;
            String endpoint = index;
            switch (esCommandType) {
                case 0:
                case 1:
                    // PUT /${index}
                    httpMethod = PUT;
                    break;
                case 2:
                    // POST /${index}/_update
                    endpoint = String.format(ENDPOINT_UPDATE_FORMAT, index);
                    break;
                case 3:
                    // DELETE /${index}
                    httpMethod = DELETE;
                    endpoint = String.format(ENDPOINT_DELETE_FORMAT, index);
                    break;
                case 4:
                    // POST /_bulk
                    endpoint = String.format(ENDPOINT_BULK_FORMAT, index);
                    break;
                case 5:
                    // POST /${index}/_update_by_query
                    endpoint = String.format(ENDPOINT_UPDATE_QUERY_FORMAT, index);
                    break;
                case 6:
                    // POST /${index}/_delete_by_query
                    endpoint = String.format(ENDPOINT_DELETE_QUERY_FORMAT, index);
                    break;
                default:
                    httpMethod = POST;
                    endpoint = index;
            }
            Response response = execute(lowLevelClient, entity, httpMethod, endpoint);
            if (response != null && (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK
                    || response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED) ) {
                result = true;
            }
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        } finally {
            closeResource(lowLevelClient, client, esSourceDTO);
            if (entity != null) {
                entity.close();
            }
        }
        return result;
    }

    private Response execute(RestClient lowLevelClient, HttpEntity entity, String httpMethod, String endpoint) throws IOException {
        Request request = new Request(httpMethod, endpoint);
        request.setEntity(entity);
        return lowLevelClient.performRequest(request);
    }

}
