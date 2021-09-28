/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.common.loader.restful.http;

import java.security.PrivilegedAction;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.dtstack.dtcenter.common.loader.common.DtClassThreadFactory;
import com.dtstack.dtcenter.common.loader.hadoop.util.KerberosLoginUtil;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;

@Slf4j
public class HttpClientFactory {

    /**
     * http IO 线程数，默认一个，后面如果需要支持池化 再做修改
     */
    private static final Integer IO_THREAD_COUNT = 1;

    /**
     * HTTP连接超时时间，单位：秒
     */
    private static final Integer HTTP_CONNECT_TIMEOUT = 90;

    /**
     * Socket 超时时间，单位：秒
     */
    private static final Integer HTTP_SOCKET_TIMEOUT = 90;

    /**
     * 获取 HTTP 连接超时时间，单位：秒
     */
    private static final Integer HTTP_CONNECTION_REQUEST_TIMEOUT = 90;

    public static HttpClient createHttpClientAndStart(ISourceDTO sourceDTO) {
        HttpClient httpClient = createHttpClient(sourceDTO);
        httpClient.start();
        return httpClient;
    }

    public static HttpClient createHttpClient(ISourceDTO sourceDTO) {
        RestfulSourceDTO restfulSourceDTO = (RestfulSourceDTO) sourceDTO;
        // 创建 ConnectingIOReactor
        ConnectingIOReactor ioReactor = initIOReactorConfig();

        // 支持 http、https
        Registry<SchemeIOSessionStrategy> sessionStrategyRegistry =
                RegistryBuilder.<SchemeIOSessionStrategy>create()
                        .register("http", NoopIOSessionStrategy.INSTANCE)
                        .register("https", SSLIOSessionStrategy.getDefaultStrategy())
                        .build();
        // 创建链接管理器
        PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor, sessionStrategyRegistry);

        // 创建HttpAsyncClient
        CloseableHttpAsyncClient httpAsyncClient = createPoolingHttpClient(cm, restfulSourceDTO.getConnectTimeout(), restfulSourceDTO.getSocketTimeout(), restfulSourceDTO.getKerberosConfig());

        // 启动定时调度
        ScheduledExecutorService clearConnService = initFixedCycleCloseConnection(cm);

        // 组合生产HttpClientImpl
        return new HttpClient(restfulSourceDTO, httpAsyncClient, clearConnService);
    }


    /**
     * 初始化 http 请求配置
     *
     * @param connectTimeout 连接超时时间
     * @param socketTimeout  socket 超时时间
     * @return http 请求配置
     */
    private static RequestConfig initRequestConfig(Integer connectTimeout, Integer socketTimeout) {
        final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        // ConnectTimeout:连接超时.连接建立时间，三次握手完成时间.
        Integer connTimeout = Objects.isNull(connectTimeout) ? HTTP_CONNECT_TIMEOUT : connectTimeout;
        Integer sockTimeout = Objects.isNull(socketTimeout) ? HTTP_SOCKET_TIMEOUT : socketTimeout;
        requestConfigBuilder.setConnectTimeout(connTimeout * 1000);
        // SocketTimeout:Socket请求超时.数据传输过程中数据包之间间隔的最大时间.
        requestConfigBuilder.setSocketTimeout(sockTimeout * 1000);
        // ConnectionRequestTimeout:httpclient使用连接池来管理连接，这个时间就是从连接池获取连接的超时时间，可以想象下数据库连接池
        requestConfigBuilder.setConnectionRequestTimeout(HTTP_CONNECTION_REQUEST_TIMEOUT * 1000);
        return requestConfigBuilder.build();
    }

    /**
     * 初始化线程数
     *
     * @return ConnectingIOReactor
     */
    private static ConnectingIOReactor initIOReactorConfig() {
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(IO_THREAD_COUNT).build();
        ConnectingIOReactor ioReactor;
        try {
            ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
            return ioReactor;
        } catch (IOReactorException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    /**
     * 初始化周期调度线程池去关闭无用连接
     *
     * @param cm connection 管理器
     * @return 周期调度线程池
     */
    private static ScheduledExecutorService initFixedCycleCloseConnection(final PoolingNHttpClientConnectionManager cm) {
        // 定时关闭所有空闲链接
        ScheduledExecutorService connectionGcService = Executors.newSingleThreadScheduledExecutor(new DtClassThreadFactory("Loader-close-connection"));
        connectionGcService.scheduleAtFixedRate(() -> {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Close idle connections, fixed cycle operation");
                }
                cm.closeIdleConnections(3, TimeUnit.MINUTES);
            } catch (Exception ex) {
                log.error("", ex);
            }
        }, 30, 30, TimeUnit.SECONDS);
        return connectionGcService;
    }

    /**
     * 创建异步 http client
     *
     * @param cm             http connection 管理器
     * @param connectTimeout 连接超时时间
     * @param socketTimeout  socket 超时时间
     * @param kerberosConfig kerberos 配置
     * @return 异步 http client
     */
    private static CloseableHttpAsyncClient createPoolingHttpClient(PoolingNHttpClientConnectionManager cm, Integer connectTimeout, Integer socketTimeout, Map<String, Object> kerberosConfig) {

        if (MapUtils.isNotEmpty(kerberosConfig)) {
            return KerberosLoginUtil.loginWithUGI(kerberosConfig).doAs(
                    (PrivilegedAction<CloseableHttpAsyncClient>) () -> {
                        RequestConfig requestConfig = initRequestConfig(connectTimeout, socketTimeout);
                        HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();

                        // 设置连接管理器
                        httpAsyncClientBuilder.setConnectionManager(cm);

                        // 设置RequestConfig
                        if (requestConfig != null) {
                            httpAsyncClientBuilder.setDefaultRequestConfig(requestConfig);
                        }

                        // 设置 kerberos 认证
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(AuthScope.ANY, new KerberosCredentials(null));
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        return httpAsyncClientBuilder.build();
                    });

        }
        RequestConfig requestConfig = initRequestConfig(connectTimeout, socketTimeout);
        HttpAsyncClientBuilder httpAsyncClientBuilder = HttpAsyncClients.custom();

        // 设置连接管理器
        httpAsyncClientBuilder.setConnectionManager(cm);

        // 设置RequestConfig
        if (requestConfig != null) {
            httpAsyncClientBuilder.setDefaultRequestConfig(requestConfig);
        }
        return httpAsyncClientBuilder.build();
    }
}
