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

package com.dtstack.dtcenter.common.loader.hbase;

import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import org.apache.hadoop.hbase.io.compress.Compression;

/**
 * 压缩方式枚举
 *
 * @author luming
 * @date 2022/2/14
 */
public enum AlgorithmType {
    LZO("lzo", Compression.Algorithm.LZO),
    GZ("gz", Compression.Algorithm.GZ),
    NONE("none", Compression.Algorithm.NONE),
    SNAPPY("snappy", Compression.Algorithm.SNAPPY),
    LZ4("lz4", Compression.Algorithm.LZ4),
    BZIP2("bzip2", Compression.Algorithm.BZIP2),
    ZSTD("zstd", Compression.Algorithm.ZSTD);

    private String key;

    private Compression.Algorithm value;

    AlgorithmType(String key, Compression.Algorithm value) {
        this.key = key;
        this.value = value;
    }

    public static Compression.Algorithm getType(String key) {
        for (AlgorithmType algorithmType : AlgorithmType.values()) {
            if (algorithmType.getKey().equalsIgnoreCase(key)) {
                return algorithmType.getType();
            }
        }

        throw new DtLoaderException("hbase not supported this compression algorithm: " + key);
    }

    public String getKey() {
        return key;
    }

    public Compression.Algorithm getType() {
        return value;
    }
}
