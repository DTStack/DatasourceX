package com.dtstack.dtcenter.common.loader.spark.client;

import com.dtstack.dtcenter.common.loader.rdbms.AbsTableClient;
import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.common.loader.spark.SparkConnFactory;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.extern.slf4j.Slf4j;

/**
 * spark表操作相关接口
 *
 * @author ：wangchuan
 * date：Created in 10:57 上午 2020/12/3
 * company: www.dtstack.com
 */
@Slf4j
public class SparkTableClient extends AbsTableClient {

    @Override
    protected ConnFactory getConnFactory() {
        return new SparkConnFactory();
    }

    @Override
    protected DataSourceType getSourceType() {
        return DataSourceType.Spark;
    }

}
