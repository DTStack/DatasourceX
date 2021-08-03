package com.dtstack.dtcenter.loader.dto;

import com.dtstack.dtcenter.loader.dto.filter.Filter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * hbase queryDTO
 *
 * @author ：wangchuan
 * date：Created in 上午10:02 2021/7/8
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HbaseQueryDTO {

    private String tableName;

    private List<String> columns;

    private Filter filter;

    private String startRowKey;

    private String endRowKey;

    private Long limit;

    private Map<String, ColumnType> columnTypes;

    public enum ColumnType {
        INT(),

        LONG(),

        STRING(),

        BIG_DECIMAL(),

        BOOLEAN(),

        DOUBLE(),

        FLOAT(),

        SHORT(),

        HEX()
    }
}
