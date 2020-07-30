package com.dtstack.dtcenter.common.loader.mysql;

import com.dtstack.dtcenter.common.loader.common.ConnFactory;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import com.dtstack.dtcenter.loader.source.DataBaseType;
import org.apache.commons.lang.StringUtils;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 17:11 2020/1/3
 * @Description：Mysql 连接
 */
public class MysqlConnFactory extends ConnFactory {
    public MysqlConnFactory() {
        driverName = DataBaseType.MySql.getDriverClassName();
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
