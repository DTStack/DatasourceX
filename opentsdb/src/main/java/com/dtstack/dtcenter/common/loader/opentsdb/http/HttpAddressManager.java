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

	private HttpAddressManager(OpenTSDBSourceDTO sourceDTO) {
		address = sourceDTO.getHost() + ":" + sourceDTO.getPort();
	}

	public static HttpAddressManager createHttpAddressManager(OpenTSDBSourceDTO sourceDTO) {
		return new HttpAddressManager(sourceDTO);
	}

	public String getAddress() {
		return this.address;
	}
}
