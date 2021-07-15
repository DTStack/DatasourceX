package com.dtstack.dtcenter.common.loader.kylinRestful.http;

public interface HttpAPI {

    /**
     * 认证
     */
    String AUTH = "/kylin/api/user/authentication";

    /**
     * 获取project
     */
    String PROJECT_LIST = "/kylin/api/projects/readable";

    /**
     * 获取kylin 表
     */
    String HIVE_TABLES = "/kylin/api/tables?ext=true&project=%s";

    /**
     * 获取hive 表详情 {project}/{tableName}
     */
    String HIVE_A_TABLE = "/kylin/api/tables/%s/%s";

    /**
     * Show databases in hive
     */
    String HIVE_LIST_ALL_DB = "/kylin/api/tables/hive?project=%s";

    /**
     * databases && project
     */
    String HIVE_LIST_ALL_TABLES = "/kylin/api/tables/hive/%s?project=%s";
}