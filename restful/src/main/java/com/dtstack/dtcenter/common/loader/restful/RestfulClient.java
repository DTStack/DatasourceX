package com.dtstack.dtcenter.common.loader.restful;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.common.loader.common.utils.AddressUtil;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.net.URI;

/**
 * restful 客户端
 *
 * @author ：wangchuan
 * date：Created in 上午10:37 2021/8/11
 * company: www.dtstack.com
 */
public class RestfulClient<T> extends AbsNoSqlClient<T> {

    private static final int UNDEFINED_PORT = -1;

    private static final int DEFAULT_PORT = 80;

    @Override
    public Boolean testCon(ISourceDTO source) {
        RestfulSourceDTO restfulSourceDTO = (RestfulSourceDTO) source;
        URI uri = URI.create(restfulSourceDTO.getUrl());
        // 默认端口 80
        int port = uri.getPort() != UNDEFINED_PORT ? uri.getPort() : DEFAULT_PORT;
        boolean telnetCheck = AddressUtil.telnet(uri.getHost(), port);
        if (!telnetCheck) {
            throw new DtLoaderException(String.format("failed to telnet host: %s and port: %s", uri.getHost(), port));
        }
        return true;
    }
}
