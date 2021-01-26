package com.dtstack.dtcenter.loader.enums;

/**
 *
 * @Description: Elasticsearch 操作
 * @Author: 景行
 * @Date: 2020/9/18
 */
public enum EsCommandType {
    /**
     * 默认操作
     */
    DEFAULT(0),
    /**
     * insert 操作，插入时要指定_id
     */
    INSERT(1),
    /**
     * _update 操作，指定_id
     */
    UPDATE(2),
    /**
     * delete操作，删除单条数据要指定_id
     */
    DELETE(3),
    /**
     * _bulk批量操作，默认请求/_bulk
     */
    BULK(4),
    /**
     * _update_by_query 根据条件更新,需要在endpoint中指定_index和_type
     */
    UPDATE_BY_QUERY(5),
    /**
     * _delete_by_query 根据条件删除,需要在endpoint中指定_index和_type
     */
    DELETE_BY_QUERY(6);

    private Integer type;

    public Integer getType() {
        return type;
    }

    EsCommandType(Integer type) {
        this.type = type;
    }
}