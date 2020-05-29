package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.enums.ConnectionClearStatus;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.sql.Connection;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 18:24 2020/5/22
 * @Description：Phoenix 数据源信息
 */
@Data
@ToString
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class PhoenixSourceDTO extends RdbmsSourceDTO {
}
