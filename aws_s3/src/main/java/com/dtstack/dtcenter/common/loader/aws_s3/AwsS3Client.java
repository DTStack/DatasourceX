package com.dtstack.dtcenter.common.loader.aws_s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;
import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.AwsS3SourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;

import java.util.List;
import java.util.stream.Collectors;

/**
 * aws s3 Client
 *
 * @author ：wangchuan
 * date：Created in 上午9:46 2021/5/6
 * company: www.dtstack.com
 */
public class AwsS3Client<T> extends AbsNoSqlClient<T> {

    @Override
    public Boolean testCon(ISourceDTO source) {
        AwsS3SourceDTO sourceDTO = AwsS3Util.convertSourceDTO(source);
        AmazonS3 amazonS3 = null;
        try {
            amazonS3 = AwsS3Util.getClient(sourceDTO);
            amazonS3.listBuckets();
        } catch (Exception e) {
            throw new DtLoaderException(String.format("aws s3 connection failed : %s", e.getMessage()), e);
        } finally {
            AwsS3Util.closeAmazonS3(amazonS3);
        }
        return true;
    }

    @Override
    public List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO) {
        AwsS3SourceDTO sourceDTO = AwsS3Util.convertSourceDTO(source);
        AmazonS3 amazonS3 = null;
        List<String> result;
        try {
            amazonS3 = AwsS3Util.getClient(sourceDTO);
            List<Bucket> buckets = amazonS3.listBuckets();
            result = buckets.stream().map(Bucket::getName).collect(Collectors.toList());
        } catch (Exception e) {
            throw new DtLoaderException(String.format("aws s3 get buckets failed : %s", e.getMessage()), e);
        } finally {
            AwsS3Util.closeAmazonS3(amazonS3);
        }
        return result;
    }

    @Override
    public List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO) {
        return getTableList(source, queryDTO);
    }
}
