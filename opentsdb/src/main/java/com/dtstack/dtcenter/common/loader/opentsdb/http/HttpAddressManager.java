package com.dtstack.dtcenter.common.loader.opentsdb.http;

import com.dtstack.dtcenter.loader.dto.source.OpenTSDBSourceDTO;

/**
 * Http 地址管理
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/17
 * company: www.dtstack.com
 */
public class HttpAddressManager {

	private final String address;

	public static final String HTTP_PREFIX = "http://";

	private static final String HTTPS_PREFIX = "https://";

	private HttpAddressManager(OpenTSDBSourceDTO sourceDTO) {
		// 默认 http 协议
		address = (sourceDTO.getUrl().startsWith(HTTP_PREFIX) || sourceDTO.getUrl().startsWith(HTTPS_PREFIX)) ?
				sourceDTO.getUrl() : HTTP_PREFIX + sourceDTO.getUrl();
	}

	public static HttpAddressManager createHttpAddressManager(OpenTSDBSourceDTO sourceDTO) {
		return new HttpAddressManager(sourceDTO);
	}

	public String getAddress() {
		return this.address;
	}
}
