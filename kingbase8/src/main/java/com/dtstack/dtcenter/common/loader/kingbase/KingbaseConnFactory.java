package com.dtstack.dtcenter.common.loader.kingbase;

import com.dtstack.dtcenter.common.loader.rdbms.ConnFactory;
import com.dtstack.dtcenter.loader.source.DataBaseType;

/**
 * company: www.dtstack.com
 * @author ：Nanqi
 * Date ：Created in 17:11 2020/09/01
 * Description：kingbase 连接工厂
 */
public class KingbaseConnFactory extends ConnFactory {
    public KingbaseConnFactory() {
        driverName = DataBaseType.KINGBASE8.getDriverClassName();
    }
}
