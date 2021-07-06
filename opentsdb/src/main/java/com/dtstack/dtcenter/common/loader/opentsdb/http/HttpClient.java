package com.dtstack.dtcenter.common.loader.opentsdb.http;

import com.dtstack.dtcenter.loader.dto.source.OpenTSDBSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class HttpClient {

    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * 实际的HttpClient
     */
    private final CloseableHttpAsyncClient httpclient;

    /**
     * 未完成任务数 for graceful close.
     */
    private final AtomicInteger unCompletedTaskNum;

    /**
     * http 地址、port 管理
     */
    private final HttpAddressManager httpAddressManager;

    /**
     * 清除空闲连接服务
     */
    private final ScheduledExecutorService clearConnService;

    HttpClient(OpenTSDBSourceDTO sourceDTO, CloseableHttpAsyncClient httpclient, ScheduledExecutorService clearConnService) {
        this.httpclient = httpclient;
        this.httpAddressManager = HttpAddressManager.createHttpAddressManager(sourceDTO);
        this.unCompletedTaskNum = new AtomicInteger(0);
        this.clearConnService = clearConnService;
    }

    public void close() throws IOException {
        this.close(false);
    }

    public void close(boolean force) throws IOException {
        // 关闭等待
        if (!force) {
            // 优雅关闭
            while (true) {
                if (httpclient.isRunning()) { // 正在运行则等待
                    int i = this.unCompletedTaskNum.get();
                    if (i == 0) {
                        break;
                    } else {
                        try {
                            // 轮询检查优雅关闭
                            TimeUnit.MILLISECONDS.sleep(50);
                        } catch (InterruptedException e) {
                            log.warn("The thread {} is Interrupted", Thread.currentThread().getName());
                        }
                    }
                } else {
                    // 已经不再运行则退出
                    break;
                }
            }
        }
        clearConnService.shutdownNow();
        // 关闭
        httpclient.close();
    }

    private HttpResponse execute(HttpEntityEnclosingRequestBase request, String json) {
        if (json != null && json.length() > 0) {
            request.addHeader("Content-Type", "application/json");
            request.setEntity(generateStringEntity(json));
        }
        unCompletedTaskNum.incrementAndGet();
        Future<HttpResponse> future = httpclient.execute(request, null);
        try {
            return future.get();
        } catch (Exception e) {
            throw new DtLoaderException(String.format("execute http request error:%s", e.getMessage()), e);
        } finally {
            unCompletedTaskNum.decrementAndGet();
        }
    }

    private StringEntity generateStringEntity(String json) {
        return new StringEntity(json, DEFAULT_CHARSET);
    }

    private String getUrl(String apiPath) {
        return this.httpAddressManager.getAddress() + apiPath;
    }

    public HttpResponse post(String apiPath, String json) {
        return this.post(apiPath, json, new HashMap<String, String>());
    }

    public HttpResponse post(String apiPath, String json, Map<String, String> params) {
        String httpFullAPI = getUrl(apiPath);
        URI uri = createURI(httpFullAPI, params);
        final HttpPost request = new HttpPost(uri);
        return execute(request, json);
    }

    private URI createURI(String httpFullAPI, Map<String, String> params) {
        URIBuilder builder;
        try {
            builder = new URIBuilder(httpFullAPI);
        } catch (URISyntaxException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }

        if (params != null && !params.isEmpty()) {
            for (Entry<String, String> entry : params.entrySet()) {
                builder.setParameter(entry.getKey(), entry.getValue());
            }
        }

        URI uri;
        try {
            uri = builder.build();
        } catch (URISyntaxException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
        return uri;
    }

    public void start() {
        this.httpclient.start();
    }
}
