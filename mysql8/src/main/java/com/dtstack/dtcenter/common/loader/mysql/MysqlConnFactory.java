package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.enums.DataBaseType;
import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import org.apache.commons.lang.StringUtils;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:01 2020/2/27
 * @Description：Mysql 连接
 */
public class MysqlConnFactory extends ConnFactory {
    public MysqlConnFactory() {
        driverName = DataBaseType.MySql8.getDriverClassName();
    }

    @Override
    protected String dealSourceUrl(RdbmsSourceDTO rdbmsSourceDTO) {
        String schema = rdbmsSourceDTO.getSchema();
        String url = rdbmsSourceDTO.getUrl();
        if (StringUtils.isNotEmpty(schema)){
            String[] urlAyy = url.split("/");
            if (urlAyy.length > 2){
                url = urlAyy[0] + "//" + urlAyy[2] + "/" +schema;
            }
        }
        return url;
    }
}
