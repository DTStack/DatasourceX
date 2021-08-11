package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.restful.Response;
import com.dtstack.dtcenter.loader.dto.source.RestfulSourceDTO;

import java.io.File;
import java.util.Map;

/**
 * <p>提供 Restful 相关操作方法</p>
 *
 * @author ：wangchuan
 * date：Created in 上午10:06 2021/8/9
 * company: www.dtstack.com
 */
public interface IRestful {

    /**
     * get 请求
     *
     * @param sourceDTO 数据源信息
     * @param params    请求参数
     * @param cookies   cookie 信息
     * @param headers   header 信息
     * @return 相应
     */
    Response get(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers);

    /**
     * post 请求
     *
     * @param sourceDTO 数据源信息
     * @param bodyData  请求参数
     * @param cookies   cookie 信息
     * @param headers   header 信息
     * @return 相应
     */
    Response post(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers);

    /**
     * delete 请求
     *
     * @param sourceDTO 数据源信息
     * @param bodyData  请求参数
     * @param cookies   cookie 信息
     * @param headers   header 信息
     * @return 相应
     */
    Response delete(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers);

    /**
     * put 请求
     *
     * @param sourceDTO 数据源信息
     * @param bodyData  body 信息
     * @param cookies   cookie 信息
     * @param headers   header 信息
     * @return 相应
     */
    Response put(RestfulSourceDTO sourceDTO, String bodyData, Map<String, String> cookies, Map<String, String> headers);

    /**
     * put Multipart
     *
     * @param sourceDTO 数据源信息
     * @param params    请求参数
     * @param cookies   cookie 信息
     * @param headers   header 信息
     * @param files     文件信息
     * @return 相应
     */
    Response postMultipart(RestfulSourceDTO sourceDTO, Map<String, String> params, Map<String, String> cookies, Map<String, String> headers, Map<String, File> files);

}
