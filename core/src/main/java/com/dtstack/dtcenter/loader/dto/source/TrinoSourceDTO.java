package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.dto.SSLConfigDTO;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * trino sourceDTO
 *
 * @author ：wangchuan
 * date：Created in 下午2:23 2021/9/7
 * company: www.dtstack.com
 */
@Data
@ToString
@SuperBuilder
public class TrinoSourceDTO extends RdbmsSourceDTO {

    /**
     * 判断是否开启 ssl 认证，不为 null 表示开启 ssl 认证
     */
    private SSLConfigDTO sslConfigDTO;

    @Override
    public Integer getSourceType() {
        return DataSourceType.TRINO.getVal();
    }
}
