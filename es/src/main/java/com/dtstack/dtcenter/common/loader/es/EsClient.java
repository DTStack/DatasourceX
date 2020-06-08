package com.dtstack.dtcenter.common.loader.es;

import com.dtstack.dtcenter.common.enums.DataSourceType;
import com.dtstack.dtcenter.common.http.PoolHttpClient;
import com.dtstack.dtcenter.common.loader.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.source.ESSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

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
     * 根据连接确定连接成功性
     *
     * @param client
     * @return
     */
    private static boolean checkConnect(RestHighLevelClient client) {
        boolean check = false;
        try {
            client.info();
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

    public static List<String> getDatabases(ISourceDTO iSource, SqlQueryDTO queryDTO){
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
        IndicesClient indices = client.indices();

        //request.
        return null;
    }

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
        GetRequest getRequest = new GetRequest(esSourceDTO.getSchema());
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest);
        }catch (Exception e){
            log.error("获取文档异常", e);
        }
        //getResponse.
        return null;
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
    public List<String> getTableList(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<String> getColumnClassInfo(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(ISourceDTO iSource, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support");
    }
}
