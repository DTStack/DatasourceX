package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.util.List;
import java.util.Map;

/**
 * hbase客户端接口
 *
 * @author ：wangchuan
 * date：Created in 9:38 上午 2020/12/2
 * company: www.dtstack.com
 */
public interface IHbase {

    /**
     * 判断namespace是否存在
     *
     * @param source 数据源信息
     * @param namespace hbase namespace
     * @return 是否存在
     */
    Boolean isDbExists(ISourceDTO source, String namespace);

    /**
     * hbase创建表
     *
     * @param source 数据源信息
     * @param tableName 表名
     * @param colFamily 列族列表
     * @return 创建结果
     */
    Boolean createHbaseTable(ISourceDTO source, String tableName, String[] colFamily);

    /**
     * hbase创建表，指定namespace
     *
     * @param source 数据源信息
     * @param namespace hbase namespace
     * @param tableName 表名
     * @param colFamily 列族列表
     * @return 创建结果
     */
    Boolean createHbaseTable(ISourceDTO source, String namespace, String tableName, String[] colFamily);

    /**
     * hbase 根据正则查询匹配的数据，返回rowkey集合
     *
     * @param source 数据源连接信息
     * @param tableName 表名
     * @param regex 匹配正则
     *
     * @return 查询结果
     */
    List<String> scanByRegex(ISourceDTO source, String tableName, String regex);

    /**
     * hbase 根据rowKey删除数据
     * @param source 数据源信息
     * @param tableName 表名
     * @param rowKeys rowkey列表
     *
     * @return 删除状态
     */
    Boolean deleteByRowKey (ISourceDTO source, String tableName, String family, String qualifier, List<String> rowKeys);

    /**
     * hbase向指定的rowKey插入数据
     *
     * @param source 数据源信息
     * @param tableName 表名
     * @param rowKey hbase rowkey
     * @param family 列族
     * @param qualifier 列名
     * @param data 数据
     * @return 数据插入状态
     */
    Boolean putRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier, String data);

    /**
     * hbase根据rowKey获取数据
     *
     * @param source 数据源信息
     * @param tableName 表名
     * @param rowKey hbase rowkey
     * @return 查询结果
     */
    String getRow(ISourceDTO source, String tableName, String rowKey, String family, String qualifier);

    /**
     * hbase 数据预览，如果有数据则第一行为字段信息，如果没有数据返回空 list
     *
     * @param source     数据源信息
     * @param tableName  表名
     * @param previewNum 预览条数，最大5000条 默认100条
     * @return 预览数据
     */
    List<List<String>> preview(ISourceDTO source, String tableName, Integer previewNum);

    /**
     * hbase 数据预览，如果有数据则第一行为字段信息，如果没有数据返回空 list
     *
     * @param source     数据源信息
     * @param tableName  表名
     * @param familyList 预览指定列族的数据，即当前行如果有满足该列族的数据就返回
     * @param previewNum 预览条数，最大5000条 默认100条
     * @return 预览数据
     */
    List<List<String>> preview(ISourceDTO source, String tableName, List<String> familyList, Integer previewNum);

    /**
     * hbase 数据预览，如果有数据则第一行为字段信息，如果没有数据返回空 list
     *
     * @param source             数据源信息
     * @param tableName          表名
     * @param familyQualifierMap 预览指定列族、列名下的数据
     * @param previewNum         预览条数，最大5000条 默认100条
     * @return 预览数据
     */
    List<List<String>> preview(ISourceDTO source, String tableName, Map<String, List<String>> familyQualifierMap, Integer previewNum);
}
