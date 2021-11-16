package com.dtstack.dtcenter.common.loader.dorisrestful.request;

public interface HttpAPI {

    /**
     * 登陆
     */
    String AUTH = "/rest/v1/login";

    /**
     * 获取所有的库 cluster
     */
    String ALL_DATABASE = "/api/meta/namespaces/%s/databases";

    /**
     * 获取库下的表 cluster schema tableName
     */
    String ALL_TABLES = "/api/meta/namespaces/%s/databases/%s:%s/tables";

    /**
     * 获取元数据 cluster cluster schema
     */
    String COLUMN_METADATA = "/api/meta/namespaces/%s/databases/%s/tables/%s/schema";

    /**
     * 数据预览 cluster schema
     */
    String QUERY_DATA = "/api/query/%s/%s";
}