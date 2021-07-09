package com.dtstack.dtcenter.common.loader.doris;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlDownloader;

import java.sql.Connection;

/**
 * @author ：qianyi
 * date：Created in 下午1:46 2021/07/09
 * company: www.dtstack.com
 */
public class DorisDownloader extends MysqlDownloader {

    public DorisDownloader(Connection connection, String sql, String schema) {
        super(connection, sql, schema);
    }
}
