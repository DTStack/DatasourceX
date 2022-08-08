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

package com.dtstack.dtcenter.common.loader.hivecdc.metadata.cons;

/**
 * @author luming
 * @date 2022/4/18
 */
public class HiveMetaCons {
    public static final String KEY_RESULTSET_COMMENT = "comment";

    public static final String KEY_TRANSIENT_LASTDDLTIME = "transient_lastDdlTime";

    public static final String TEXT_FORMAT = "TextOutputFormat";
    public static final String ORC_FORMAT = "OrcOutputFormat";
    public static final String PARQUET_FORMAT = "MapredParquetOutputFormat";

    public static final String TYPE_TEXT = "TEXTFILE";
    public static final String TYPE_ORC = "orc";
    public static final String TYPE_PARQUET = "parquet";
}
