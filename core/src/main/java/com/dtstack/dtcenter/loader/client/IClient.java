package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.Table;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 09:27 2020/1/13
 * @Description：客户端接口
 */
public interface IClient<T> {
    /**
     * 获取 连接
     *
     * @param source 数据源信息
     * @return
     * @throws Exception
     */
    Connection getCon(ISourceDTO source);

    /**
     * 获取 连接
     *
     * @param source 数据源信息
     * @param taskParams 任务环境变量
     *
     * @return
     * @throws Exception
     */
    Connection getCon(ISourceDTO source, String taskParams);

    /**
     * 校验 连接
     *
     * @param source
     * @return
     * @throws Exception
     */
    Boolean testCon(ISourceDTO source);

    /**
     * 执行查询
     *
     * @param source
     * @param queryDTO 必填项 sql
     * @return
     * @throws Exception
     */
    List<Map<String, Object>> executeQuery(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 执行查询，无需结果集
     *
     * @param source
     * @param queryDTO 必填项 sql
     * @return
     * @throws Exception
     */
    Boolean executeSqlWithoutResultSet(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 返回所有的表字段名称
     * 是否视图表，默认不过滤
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<String> getTableList(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 根据schema/db获取表
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<String> getTableListBySchema(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 返回字段 Java 类的标准名称
     * 字段名若不填则默认全部
     *
     * @param source
     * @param queryDTO 必填项 表名
     * @return
     * @throws Exception
     */
    List<String> getColumnClassInfo(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取表字段属性
     * 字段名若不填则默认全部, 是否过滤分区字段 不填默认不过滤
     *
     * @param source
     * @param queryDTO 必填项 表名
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 根据自定义sql获取表字段属性
     *
     * @param source
     * @param queryDTO 必填项 sql
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getColumnMetaDataWithSql(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取flinkSql字段名称
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getFlinkColumnMetaData(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取表注释
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    String getTableMetaComment(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取预览数据
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<List<Object>> getPreview(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取对应的downloader
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    IDownloader getDownloader(ISourceDTO source, SqlQueryDTO queryDTO) throws Exception;

    /**
     * 根据执行 sql 下载数据
     *
     * @param source   数据源信息
     * @param sql      执行 sql
     * @param pageSize 每页的数量
     * @return 数据下载 downloader
     * @throws Exception 异常
     */
    IDownloader getDownloader(ISourceDTO source, String sql, Integer pageSize) throws Exception;

    /**
     * 获取所有的 DB
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<String> getAllDatabases(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取所有的db，此方法目的为获取所有的 database，为了解决一些遗留问题。例如 oracle 12 后支持 cdb
     * 模式，此时 oracle 可以包含多个 pdb，每个 pdb 下面可以有多个 schema，但是getAllDatabases 返回
     * 的是 schema 列表
     *
     * @param source   数据源信息
     * @param queryDTO 查询信息
     * @return db 列表
     * @see IClient#getAllDatabases 该方法对于有 database 概念的数据源返回的是 database，否则返回 schema
     */
    List<String> getRootDatabases(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取建表语句
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    String getCreateTableSql(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取分区字段
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getPartitionColumn(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取表信息
     *
     * @param source
     * @param queryDTO
     * @return
     * @throws Exception
     */
    Table getTable(ISourceDTO source, SqlQueryDTO queryDTO);

    /**
     * 获取当前使用的数据库
     *
     * @param source 数据源信息
     * @return 当前使用的数据库
     * @throws Exception
     */
    String getCurrentDatabase(ISourceDTO source);

    /**
     * 创建数据库
     *
     * @param source 数据源信息
     * @param dbName 需要创建的数据库
     * @param comment 库注释信息
     * @return 创建结果
     * @throws Exception
     */
    Boolean createDatabase(ISourceDTO source, String dbName, String comment);

    /**
     * 判断数据库是否存在
     *
     * @param source 数据源信息
     * @param dbName 数据库名称
     * @return 判断结果
     * @throws Exception
     */
    Boolean isDatabaseExists(ISourceDTO source, String dbName) ;

    /**
     * 判断该数据库中是否存在该表
     *
     * @param source 数据源信息
     * @param tableName 表名
     * @param dbName 库名
     * @return 判断结果
     * @throws Exception
     */
    Boolean isTableExistsInDatabase(ISourceDTO source, String tableName, String dbName);

    /**
     * 获取数据源/数据库目录列表，目前presto使用，后续pgSql等可以实现该方法用于获取所有库
     *
     * @param source
     * @return 数据源目录
     */
    List<String> getCatalogs(ISourceDTO source);

    /**
     * 获取当前数据源的版本
     *
     * @param source 数据源信息
     * @return 数据源版本
     */
    String getVersion(ISourceDTO source);

    /**
     * 列出指定路径下的文件
     *
     * @param sourceDTO  数据源信息
     * @param path       路径
     * @param includeDir 是否包含文件夹
     * @param recursive  是否递归
     * @param maxNum     最大返回条数
     * @param regexStr   正则匹配
     * @return 文件集合
     */
    List<String> listFileNames(ISourceDTO sourceDTO, String path, Boolean includeDir, Boolean recursive, Integer maxNum, String regexStr);
}
