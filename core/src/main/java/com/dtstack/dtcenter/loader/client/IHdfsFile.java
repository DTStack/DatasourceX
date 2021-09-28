/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.dtcenter.loader.client;

import com.dtstack.dtcenter.loader.IDownloader;
import com.dtstack.dtcenter.loader.dto.ColumnMetaDTO;
import com.dtstack.dtcenter.loader.dto.FileStatus;
import com.dtstack.dtcenter.loader.dto.HDFSContentSummary;
import com.dtstack.dtcenter.loader.dto.HdfsWriterDTO;
import com.dtstack.dtcenter.loader.dto.SqlQueryDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;
import com.dtstack.dtcenter.loader.enums.FileFormat;

import java.util.List;
import java.util.Map;

/**
 * @company: www.dtstack.com
 * @Author ：Nanqi
 * @Date ：Created in 14:33 2020/8/10
 * @Description：HDFS 文件操作
 */
public interface IHdfsFile {
    /**
     * 获取 HDFS 对应地址文件信息
     *
     * @param source
     * @param location
     * @return
     * @throws Exception
     */
    FileStatus getStatus(ISourceDTO source, String location);

    /**
     * 日志下载器
     *
     * @param iSource
     * @param queryDTO
     * @return
     * @throws Exception
     */
    IDownloader getLogDownloader(ISourceDTO iSource, SqlQueryDTO queryDTO);

    /**
     * 文件下载器
     *
     * @param iSource
     * @param path
     * @return
     * @throws Exception
     */
    IDownloader getFileDownloader(ISourceDTO iSource, String path);

    /**
     * 从 HDFS 上下载文件或文件夹到本地
     *
     * @param source
     * @param remotePath
     * @param localDir
     * @return
     * @throws Exception
     */
    boolean downloadFileFromHdfs(ISourceDTO source, String remotePath, String localDir);

    /**
     * 上传文件到 HDFS
     *
     * @param source
     * @param localFilePath
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean uploadLocalFileToHdfs(ISourceDTO source, String localFilePath, String remotePath);

    /**
     * 上传字节流到 HDFS
     *
     * @param source
     * @param bytes
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean uploadInputStreamToHdfs(ISourceDTO source, byte[] bytes, String remotePath);

    /**
     * 创建 HDFS 路径
     *
     * @param source
     * @param remotePath
     * @param permission
     * @return
     * @throws Exception
     */
    boolean createDir(ISourceDTO source, String remotePath, Short permission);

    /**
     * 路径文件是否存在
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean isFileExist(ISourceDTO source, String remotePath);

    /**
     * 文件检测并删除
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean checkAndDelete(ISourceDTO source, String remotePath);

    /**
     * 直接删除目标路径文件
     *
     * @param source 数据源信息
     * @param remotePath 目标路径
     * @param recursive 是否递归删除
     * @return 删除结果
     * @throws Exception
     */
    boolean delete(ISourceDTO source, String remotePath, boolean recursive);

    /**
     * 获取路径文件大小
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    long getDirSize(ISourceDTO source, String remotePath);

    /**
     * 删除文件
     *
     * @param source
     * @param fileNames
     * @return
     * @throws Exception
     */
    boolean deleteFiles(ISourceDTO source, List<String> fileNames);

    /**
     * 路径目录是否存在
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    boolean isDirExist(ISourceDTO source, String remotePath);

    /**
     * 设置路径权限
     *
     * @param source
     * @param remotePath
     * @param mode
     * @return
     * @throws Exception
     */
    boolean setPermission(ISourceDTO source, String remotePath, String mode);

    /**
     * 重命名
     *
     * @param source
     * @param src
     * @param dist
     * @return
     * @throws Exception
     */
    boolean rename(ISourceDTO source, String src, String dist);

    /**
     * 复制文件
     *
     * @param source
     * @param src
     * @param dist
     * @param isOverwrite
     * @return
     * @throws Exception
     */
    boolean copyFile(ISourceDTO source, String src, String dist, boolean isOverwrite);

    /**
     * 复制文件夹
     *
     * @param source 数据源信息
     * @param src 原路径
     * @param dist 目标路径
     * @throws Exception
     */
    boolean copyDirector(ISourceDTO source, String src, String dist);

    /**
     * 合并小文件
     *
     * @param source 数据源信息
     * @param src 合并路径
     * @param mergePath 目标路径
     * @param fileFormat 文件类型 ： text、orc、parquet {@link com.dtstack.dtcenter.loader.enums.FileFormat}
     * @param maxCombinedFileSize 合并后的文件大小
     * @param needCombineFileSizeLimit 小文件的最大值，超过此阈值的小文件不会合并
     * @throws Exception
     */
    boolean fileMerge(ISourceDTO source, String src, String mergePath, FileFormat fileFormat, Long maxCombinedFileSize, Long needCombineFileSizeLimit);

    /**
     * 获取目录或者文件的属性
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    List<FileStatus> listStatus(ISourceDTO source, String remotePath);

    /**
     * 获取目录下所有文件
     *
     * @param source
     * @param remotePath
     * @return
     * @throws Exception
     */
    List<String> listAllFilePath(ISourceDTO source, String remotePath);

    /**
     * 获取目录下所有文件的属性集
     *
     * @param source
     * @param remotePath
     * @param isIterate  是否递归
     * @return
     * @throws Exception
     */
    List<FileStatus> listAllFiles(ISourceDTO source, String remotePath, boolean isIterate);

    /**
     * 从hdfs copy文件到本地
     *
     * @param source
     * @param srcPath
     * @param dstPath
     * @return
     * @throws Exception
     */
    boolean copyToLocal(ISourceDTO source, String srcPath, String dstPath);

    /**
     * 从本地copy到hdfs
     *
     * @param source
     * @param srcPath
     * @param dstPath
     * @param overwrite 是否重写
     * @return
     * @throws Exception
     */
    boolean copyFromLocal(ISourceDTO source, String srcPath, String dstPath, boolean overwrite);

    /**
     * 根据文件格式获取对应的downlaoder
     *
     * @param source
     * @param tableLocation
     * @param fieldDelimiter
     * @param fileFormat
     * @return
     * @throws Exception
     */
    IDownloader getDownloaderByFormat(ISourceDTO source, String tableLocation, List<String> columnNames, String fieldDelimiter, String fileFormat);

    /**
     * 根据文件格式获取对应的downlaoder
     *
     * @param source          数据源信息
     * @param tableLocation   表路径
     * @param allColumns      所有字段信息
     * @param filterColumns   需要查询的字段信息
     * @param filterPartition 过滤分区信息 key：分区字段, value: 分区值
     * @param partitions      分区表所有分区(如果是分区表需要传过来,不然有可能会查询到非关联分区),也可以用来过滤分区
     * @param fieldDelimiter  字段分隔符
     * @param fileFormat      存储类型
     * @return 数据下载器
     */
    IDownloader getDownloaderByFormatWithType(ISourceDTO source, String tableLocation,
                                              List<ColumnMetaDTO> allColumns, List<String> filterColumns, Map<String, String> filterPartition,
                                              List<String> partitions, String fieldDelimiter, String fileFormat);

    /**
     * 获取hdfs上存储文件的字段信息
     *
     * @param source
     * @param queryDTO
     * @param fileFormat
     * @return
     * @throws Exception
     */
    List<ColumnMetaDTO> getColumnList(ISourceDTO source, SqlQueryDTO queryDTO, String fileFormat);

    /**
     * 按位置写入
     *
     * @param source
     * @param hdfsWriterDTO
     * @return
     * @throws Exception
     */
    int writeByPos(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO);

    /**
     * 从文件中读取行,根据提供的分隔符号分割,再根据提供的hdfs分隔符合并,写入hdfs
     * ---需要根据column信息判断导入的数据是否符合要求
     *
     * @param source
     * @param hdfsWriterDTO
     * @return
     * @throws Exception
     */
    int writeByName(ISourceDTO source, HdfsWriterDTO hdfsWriterDTO);

    /**
     * 批量统计文件夹内容摘要，包括文件的数量，文件夹的数量，文件变动时间，以及这个文件夹的占用存储等内容
     *
     * @param source 数据源信息
     * @param hdfsDirPaths hdfs上文件路径集合
     * @return 文件摘要信息
     */
    List<HDFSContentSummary> getContentSummary (ISourceDTO source, List<String> hdfsDirPaths);

    /**
     * 统计文件夹内容摘要，包括文件的数量，文件夹的数量，文件变动时间，以及这个文件夹的占用存储等内容
     *
     * @param source 数据源信息
     * @param hdfsDirPath hdfs上文件路径
     * @return 文件摘要信息
     */
    HDFSContentSummary getContentSummary (ISourceDTO source, String hdfsDirPath);


}
