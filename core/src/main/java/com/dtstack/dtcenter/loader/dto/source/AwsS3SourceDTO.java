package com.dtstack.dtcenter.loader.dto.source;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Connection;

/**
 * aws s3 sourceDTO
 *
 * @author ：wangchuan
 * date：Created in 上午9:51 2021/5/6
 * company: www.dtstack.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AwsS3SourceDTO implements ISourceDTO {

    /**
     * aws s3 文件访问密钥
     */
    private String accessKey;

    /**
     * aws s3 密钥
     */
    private String secretKey;

    /**
     * 桶所在区
     */
    private String region;

    @Override
    public Integer getSourceType() {
        return DataSourceType.AWS_S3.getVal();
    }

    @Override
    public String getUsername() {
        throw new DtLoaderException("This method is not supported");
    }

    @Override
    public String getPassword() {
        throw new DtLoaderException("This method is not supported");
    }

    @Override
    public Connection getConnection() {
        throw new DtLoaderException("This method is not supported");
    }

    @Override
    public void setConnection(Connection connection) {
        throw new DtLoaderException("This method is not supported");
    }
}
