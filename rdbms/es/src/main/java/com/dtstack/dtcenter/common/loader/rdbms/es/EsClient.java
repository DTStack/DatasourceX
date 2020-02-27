package com.dtstack.dtcenter.common.loader.rdbms.es;

import com.dtstack.dtcenter.common.loader.rdbms.common.AbsRdbmsClient;
import com.dtstack.dtcenter.common.loader.rdbms.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SourceDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 21:46 2020/2/27
 * @Description：ES 客户端
 */
public class EsClient extends AbsRdbmsClient {
    @Override
    protected ConnFactory getConnFactory() {
        return null;
    }

    @Override
    public Boolean testCon(SourceDTO source) {
        if (source == null || StringUtils.isBlank(source.getUrl())) {
            return false;
        }

        if (StringUtils.isNotBlank(source.getUsername())) {
            return checkConnectionWithPwd(source.getUrl(), source.getUsername(), source.getPassword());
        }

        return checkConnection(source.getUrl());
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
            logger.error("", e);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    logger.error("", e);
                }
            }
        }
        return check;
    }

    /**
     * 根据地址确认连接性
     * @param address
     * @return
     */
    private static boolean checkConnection(String address) {
        RestHighLevelClient client = getClient(address);
        return checkConnect(client);
    }

    /**
     * 根据用户名密码确认连接性
     * @param address
     * @param username
     * @param password
     * @return
     */
    private static boolean checkConnectionWithPwd(String address, String username, String password) {
        RestHighLevelClient client = getClient(address, username, password);
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
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.valueOf(pair[1]), "http"));
        }
        RestHighLevelClient client =
                new RestHighLevelClient(RestClient.builder(httpHostList.toArray(new HttpHost[httpHostList.size()]))
                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                            @Override
                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                            }
                        }));
        return client;
    }

    /**
     * 根据地质获取连接
     *
     * @param address
     * @return
     */
    private static RestHighLevelClient getClient(String address) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] addr = address.split(",");
        for (String add : addr) {
            String[] pair = add.split(":");
            httpHostList.add(new HttpHost(pair[0], Integer.valueOf(pair[1]), "http"));
        }
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(httpHostList.toArray(new HttpHost[httpHostList.size()])));
        return client;
    }

    /******************** 未支持的方法 **********************/
    @Override
    public Connection getCon(SourceDTO source) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name());
    }

    @Override
    public List<Map<String, Object>> executeQuery(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name());
    }

    @Override
    public Boolean executeSqlWithoutResultSet(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name());
    }

    @Override
    public List<String> getTableList(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name());
    }

    @Override
    public List<String> getColumnClassInfo(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name());
    }

    @Override
    public List<ColumnMetaDTO> getColumnMetaData(SourceDTO source, SqlQueryDTO queryDTO) throws Exception {
        throw new DtLoaderException("Not Support " + source.getSourceType().name());
    }
}
