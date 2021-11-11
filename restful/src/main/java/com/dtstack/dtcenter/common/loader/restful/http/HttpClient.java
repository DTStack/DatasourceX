package com.dtstack.dtcenter.common.loader.restful.http;

import com.dtstack.dtcenter.common.loader.restful.http.request.HttpAddressManager;
import com.dtstack.dtcenter.common.loader.restful.http.request.HttpDeleteWithEntity;
import com.dtstack.dtcenter.common.loader.restful.http.request.HttpGetWithEntity;
import com.dtstack.dtcenter.common.loader.restful.http.request.HttpPutWithEntity;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class HttpClient implements Closeable {

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
     * 清除空闲连接服务
     */
    private final ScheduledExecutorService clearConnService;

    private final HttpAddressManager httpAddressManager;

    private String authorization;

    /**
     * header 集合
     */
    private final Map<String, String> headers;

    HttpClient(RestfulSourceDTO sourceDTO, CloseableHttpAsyncClient httpclient, ScheduledExecutorService clearConnService) {
        this.httpAddressManager = HttpAddressManager.createHttpAddressManager(sourceDTO);
        this.headers = sourceDTO.getHeaders();
        this.httpclient = httpclient;
        this.unCompletedTaskNum = new AtomicInteger(0);
        this.clearConnService = clearConnService;
        //用户名密码base64加密
        String userName = StringUtils.isEmpty(sourceDTO.getUsername()) ? "" : sourceDTO.getUsername();
        String password = StringUtils.isEmpty(sourceDTO.getPassword()) ? "" : sourceDTO.getPassword();
        this.authorization = Base64.getEncoder().encodeToString((userName + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    @Override
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

    /**
     * 处理 http response
     *
     * @param httpResponse http 相应
     * @return 处理后封装的的 Response
     */
    private Response handleResponse(HttpResponse httpResponse) {
        StatusLine statusLine = httpResponse.getStatusLine();
        Response response = Response.builder()
                .statusCode(statusLine.getStatusCode())
                .build();
        HttpEntity entity = httpResponse.getEntity();
        try {
            String content;
            Header[] headers = httpResponse.getHeaders("Content-Encoding");
            // 兼容 gzip 压缩
            if (headers != null && headers.length > 0 && headers[0].getValue().equalsIgnoreCase("gzip")) {
                GzipDecompressingEntity gzipEntity = new GzipDecompressingEntity(entity);
                content = EntityUtils.toString(gzipEntity, HttpClient.DEFAULT_CHARSET);
            } else {
                content = EntityUtils.toString(entity, HttpClient.DEFAULT_CHARSET);
            }
            response.setContent(content);
        } catch (Exception e) {
            handleException(response, "Failed to parse HttpEntity", e);
        }
        return response;
    }

    /**
     * post 请求
     *
     * @param params  params 信息
     * @param cookies cookie 信息
     * @param headers header 信息
     * @return response
     */
    public Response get(Map<String, String> params, Map<String, String> cookies, Map<String, String> headers) {
        HttpGetWithEntity request = new HttpGetWithEntity(createURI(params));
        setHeaderAndCookie(request, cookies, headers);
        return execute(request, null);
    }

    /**
     * post 请求
     *
     * @param bodyData body 信息
     * @param cookies  cookie 信息
     * @param headers  header 信息
     * @return response
     */
    public Response post(String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        HttpPost request = new HttpPost(httpAddressManager.getAddress());
        setHeaderAndCookie(request, cookies, headers);
        return execute(request, bodyData);
    }

    /**
     * delete 请求
     *
     * @param bodyData body 信息
     * @param cookies  cookie 信息
     * @param headers  header 信息
     * @return response
     */
    public Response delete(String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        HttpDeleteWithEntity request = new HttpDeleteWithEntity(httpAddressManager.getAddress());
        setHeaderAndCookie(request, cookies, headers);
        return execute(request, bodyData);
    }

    /**
     * put 请求
     *
     * @param bodyData body 信息
     * @param cookies  cookie 信息
     * @param headers  header 信息
     * @return response
     */
    public Response put(String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        HttpPutWithEntity request = new HttpPutWithEntity(httpAddressManager.getAddress());
        setHeaderAndCookie(request, cookies, headers);
        return execute(request, bodyData);
    }

    /**
     * post 请求 Multipart
     *
     * @param params  params 信息
     * @param cookies cookie 信息
     * @param headers header 信息
     * @param files   文件信息
     * @return response
     */
    public Response postMultipart(Map<String, String> params, Map<String, String> cookies, Map<String, String> headers, Map<String, File> files) {
        HttpPost request = new HttpPost(httpAddressManager.getAddress());
        setHeaderAndCookie(request, cookies, headers);
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        if (MapUtils.isNotEmpty(files)) {
            // 上传的文件
            for (Map.Entry<String, File> file : files.entrySet()) {
                builder.addBinaryBody(file.getKey(), file.getValue(), ContentType.MULTIPART_FORM_DATA.withCharset(DEFAULT_CHARSET), file.getValue().getName());
            }
        }
        if (MapUtils.isNotEmpty(params)) {
            // 设置其他参数
            for (Map.Entry<String, String> entry : params.entrySet()) {
                builder.addTextBody(entry.getKey(), entry.getValue(), ContentType.MULTIPART_FORM_DATA.withCharset(DEFAULT_CHARSET));
            }
        }
        request.setEntity(builder.build());
        return execute(request, null);
    }

    /**
     * 设置 header 和 cookie
     *
     * @param request 请求
     * @param cookies cookie
     * @param headers header
     */
    public void setHeaderAndCookie(HttpEntityEnclosingRequestBase request, Map<String, String> cookies, Map<String, String> headers) {
        if (MapUtils.isNotEmpty(cookies)) {
            request.addHeader("Cookie", getCookieFormat(cookies));
        }
        // 先取传参里的 header，再取 sourceDTO 里的 header
        if (MapUtils.isNotEmpty(headers)) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.addHeader(entry.getKey(), String.valueOf(entry.getValue()));
            }
        } else if (MapUtils.isNotEmpty(this.headers)) {
            for (Map.Entry<String, String> entry : this.headers.entrySet()) {
                request.addHeader(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
    }

    /**
     * 执行请求
     *
     * @param request  请求
     * @param bodyData body 参数
     * @return Response
     */
    private Response execute(HttpEntityEnclosingRequestBase request, String bodyData) {

        request.addHeader("Content-Type", "application/json");
        request.addHeader("Authorization", "Basic " +  authorization);
        // body 不为空时设置 entity
        if (StringUtils.isNotEmpty(bodyData)) {
            request.setEntity(generateStringEntity(bodyData));
        }
        unCompletedTaskNum.incrementAndGet();
        Future<HttpResponse> future = httpclient.execute(request, null);
        try {
            HttpResponse httpResponse = future.get();
            return handleResponse(httpResponse);
        } catch (Throwable e) {
            Response errResponse = Response.builder()
                    .build();
            handleException(errResponse, "execute http request error", e);
            return errResponse;
        } finally {
            unCompletedTaskNum.decrementAndGet();
        }
    }

    /**
     * 生成 StringEntity
     *
     * @param bodyData body 请求
     * @return StringEntity
     */
    private StringEntity generateStringEntity(String bodyData) {
        return new StringEntity(bodyData, DEFAULT_CHARSET);
    }

    /**
     * 格式化 cookie
     *
     * @param cookies cookie 信息
     * @return cookie
     */
    public static String getCookieFormat(Map<String, String> cookies) {
        StringBuilder sb = new StringBuilder();
        Set<Entry<String, String>> sets = cookies.entrySet();
        for (Map.Entry<String, String> s : sets) {
            String value = Objects.isNull(s.getValue()) ? "" : s.getValue();
            sb.append(s.getKey()).append("=").append(value).append(";");
        }
        return sb.toString();
    }

    /**
     * 处理异常信息
     *
     * @param response  封装后的 response
     * @param errPrefix 异常信息前缀
     * @param e         异常
     */
    private void handleException(Response response, String errPrefix, Throwable e) {
        log.error(errPrefix, e);
        response.setStatusCode(-1);
        response.setErrorMsg(errPrefix + ":" + e.getMessage());
    }

    /**
     * 创建 URI
     *
     * @param params 请求参数
     * @return uri
     */
    private URI createURI(Map<String, String> params) {
        URIBuilder builder;
        try {
            builder = new URIBuilder(httpAddressManager.getAddress());
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
