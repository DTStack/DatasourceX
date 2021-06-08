package com.dtstack.dtcenter.common.loader.common.utils;

import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 处理模糊查询和限制调试工具类
 *
 * @author ：wangchuan
 * date：Created in 下午1:57 2021/6/7
 * company: www.dtstack.com
 */
public class SearchUtil {

    /**
     * 处理结果集，进行模糊查询和条数限制
     *
     * @param result   结果集
     * @param queryDTO 查询条件
     */
    public static List<String> handleSearchAndLimit(List<String> result, SqlQueryDTO queryDTO) {
        if (CollectionUtils.isEmpty(result) || Objects.isNull(queryDTO)) {
            return result;
        }
        if (StringUtils.isNotBlank(queryDTO.getTableNamePattern())) {
            result = result.stream().filter(single -> StringUtils.containsIgnoreCase(single, queryDTO.getTableNamePattern().trim())).collect(Collectors.toList());
        }
        if (Objects.nonNull(queryDTO.getLimit())) {
            result = result.stream().limit(queryDTO.getLimit()).collect(Collectors.toList());
        }
        return result;
    }
}
