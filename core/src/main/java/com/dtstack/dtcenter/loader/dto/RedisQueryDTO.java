package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.loader.enums.RedisCompareOp;
import com.dtstack.dtcenter.loader.enums.RedisDataType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

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
     * redis，模糊查询限制key的条数
     */
    private Integer keyLimit;

    /**
     * redis，查询结果条数限制， 只针对list,zset
     */
    private Integer ResultLimit;

    /**
     * redis 数据类型
     */
    @NonNull
    private RedisDataType redisDataType;

    /**
     * 操作符,= or like
     */
    private RedisCompareOp redisCompareOp;

    /**
     * keys，
     */
    private List<String> keys;


    /**
     * keys 模糊查询
     */
    private String keyPattern;

}
