package com.dtstack.dtcenter.common.loader.restful;

import com.dtstack.dtcenter.common.loader.restful.http.HttpClient;
import com.dtstack.dtcenter.common.loader.restful.http.HttpClientFactory;
import com.dtstack.dtcenter.loader.client.IRestful;
import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * restful 特有客户端
 *
 * @author ：wangchuan
 * date：Created in 上午10:38 2021/8/11
 * company: www.dtstack.com
 */
public class RestfulSpecialClient implements IRestful {

    @Override
    public Response get(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.get(params, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response post(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.post(bodyData, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response delete(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.delete(bodyData, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response put(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.put(bodyData, cookies, headers);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }

    @Override
    public Response postMultipart(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers, Map<String, File> files) {
        try (HttpClient httpClient = HttpClientFactory.createHttpClientAndStart(sourceDTO)) {
            return httpClient.postMultipart(params, cookies, headers, files);
        } catch (IOException e) {
            throw new DtLoaderException(e.getMessage(), e);
        }
    }
}
