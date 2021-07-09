package com.dtstack.dtcenter.common.loader.doris;

import com.dtstack.dtcenter.common.loader.mysql5.MysqlConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * @author ：qianyi
 * date：Created in 下午1:46 2021/07/09
 * company: www.dtstack.com
 */
public class DorisConnFactory extends MysqlConnFactory {

    public DorisConnFactory() {
        driverName = DataBaseType.Doris.getDriverClassName();
        this.errorPattern = new DorisErrorPattern();
    }
}
