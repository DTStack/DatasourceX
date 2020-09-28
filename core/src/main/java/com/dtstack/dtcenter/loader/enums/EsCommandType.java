package com.dtstack.dtcenter.loader.enums;

/**
 *
 * @Description: Elasticsearch 操作
 * @Author: 景行
 * @Date: 2020/9/18
 */
public enum EsCommandType {
    /**
     * insert 操作，插入时要指定_id
     */
    INSERT(0),
    /**
     * _update 操作，指定_id
     */
    UPDATE(1),
    /**
     * delete操作，删除单条数据要指定_id
     */
    DELETE(2),
    /**
     * _bulk批量操作，默认请求/_bulk
     */
    BULK(3);

    private Integer type;

    public Integer getType() {
        return type;
    }

    EsCommandType(Integer type) {
        this.type = type;
    }
}