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
package com.dtstack.dtcenter.common.loader.hdfs.util;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtils {


    public static void backupDirector(Path sourcePath, Path backupPath, FileSystem fs, Configuration configuration) throws IOException {
        if (isExists(fs, sourcePath)) {
            //判断是不是文件夹
            if (fs.isDirectory(sourcePath)) {
                if (!FileUtil.copy(fs, sourcePath, fs, backupPath, false, configuration)) {
                    throw new DtLoaderException("copy " + sourcePath.toString() + " to " + backupPath.toString() + " failed");
                }
            } else {
                throw new DtLoaderException(sourcePath.toString() + "is not a directory");
            }
        } else {
            throw new DtLoaderException(sourcePath.toString() + " is not exists");
        }
    }

    public static void backupFile(Path sourcePath, Path backupPath, FileSystem fs, Configuration configuration) throws IOException {
        //判断是不是文件
        if (isExists(fs, sourcePath)) {
            if (fs.isFile(sourcePath)) {
                if (!FileUtil.copy(fs, sourcePath, fs, backupPath, false, configuration)) {
                    throw new DtLoaderException("copy " + sourcePath.toString() + " to " + backupPath.toString() + " failed");
                }
            } else {
                throw new DtLoaderException(sourcePath.toString() + "is not a file");
            }

        } else {
            throw new DtLoaderException(sourcePath.toString() + " is not exists");
        }
    }

    public static boolean isExists(FileSystem fs, Path path) throws IOException {
        return fs.exists(path);
    }


    public static List<Path> getPaths(List<FileStatus> fileStatus) {
        return fileStatus.stream()
                .map(FileStatus::getPath).collect(Collectors.toList());
    }
}
