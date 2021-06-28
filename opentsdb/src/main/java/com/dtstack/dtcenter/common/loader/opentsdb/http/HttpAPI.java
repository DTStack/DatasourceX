package com.dtstack.dtcenter.common.loader.opentsdb.http;

/**
 * OpenTSDB Http API 接口
 *
 * @author ：wangchuan
 * date：Created in 上午10:33 2021/6/17
 * company: www.dtstack.com
 */
public interface HttpAPI {

    String PUT = "/api/put";

    String QUERY = "/api/query";

    String DELETE_DATA = "/api/delete_data";

    String DELETE_META = "/api/delete_meta";

    String SUGGEST = "/api/suggest";

    String VERSION = "/api/version";

    String VIP_HEALTH = "/api/vip_health";
}