package com.dtstack.dtcenter.common.loader.kylinRestful.http;

import com.dtstack.dtcenter.loader.dto.source.KylinRestfulSourceDTO;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HttpClientFactory {
    public static HttpClient createHttpClient(KylinRestfulSourceDTO sourceDTO) {
        return new HttpClient(sourceDTO);
    }
}
