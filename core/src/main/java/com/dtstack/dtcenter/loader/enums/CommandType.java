package com.dtstack.dtcenter.loader.enums;

/**
 * @Description: 命令操作
 * @Author: qianyi
 * @Date: 2021/6/10
 */
public enum CommandType {
    /**
     * insert 操作
     */
    INSERT(1),
    /**
     * update 操作
     */
    UPDATE(2),
    /**
     * delete操作
     */
    DELETE(3);

    private Integer type;

    public Integer getType() {
        return type;
    }

    CommandType(Integer type) {
        this.type = type;
    }
}