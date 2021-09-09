package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.loader.enums.RedisCompareOp;
import com.dtstack.dtcenter.loader.enums.RedisDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * redis queryDTO
 *
 * @author ：qianyi
 * date：Created in 上午15:02 2021/9/8
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RedisQueryDTO {

    /**
     * redis，executorQuery 开始行
     */
    private Integer startRow;

    /**
     * redis，executorQuery 限制条数
     */
    private Integer limit;

    /**
     * redis 数据类型
     */
    private RedisDataType redisDataType;

    /**
     * 操作符
     */
    private RedisCompareOp redisCompareOp;

    /**
     * keys
     */
    private List<String> keys;

}
