package com.dtstack.dtcenter.loader.dto;

import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;
import java.util.List;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:40 2020/2/26
 * @Description：查询信息
 */
@Data
@Builder
public class SqlQueryDTO {
    /**
     * 查询 SQL
     */
    private String sql;

    /**
     * 表名称
     */
    private String tableName;

    /**
     * 表名称(正则)
     */
    private String tableNamePattern;

    /**
     * 模式即 DBName 或者 kylin 的 Project
     */
    private String schema;

    /**
     * 模式即 DBName(正则)
     */
    private String schemaPattern;

    /**
     * 表类型 部分支持，建议只使用 view 这个字段
     * {@link java.sql.DatabaseMetaData#getTableTypes()}
     */
    private String[] tableTypes;

    /**
     * 字段名称
     */
    private List<String> columns;

    /**
     * 是否需要视图表，默认 false 不过滤
     */
    private Boolean view = false;

    /**
     * 是否过滤分区字段，默认 false 不过滤
     */
    private Boolean filterPartitionColumns;

    public Boolean getView() {
        if (ArrayUtils.isEmpty(getTableTypes())) {
            return Boolean.TRUE.equals(view);
        }

        return Arrays.stream(getTableTypes()).filter( type -> "VIEW".equalsIgnoreCase(type)).findFirst().isPresent();
    }

    public Boolean getFilterPartitionColumns() {
        return Boolean.TRUE.equals(filterPartitionColumns);
    }
}