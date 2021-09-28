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

package com.dtstack.dtcenter.common.loader.opentsdb.query;

import com.dtstack.dtcenter.common.loader.opentsdb.type.JSONValue;

import java.util.List;
import java.util.Map;

public class Timeline extends JSONValue {

    private String metric;

    private Map<String, String> tags;

    private List<String> fields;

    public Timeline(String metric, Map<String, String> tags) {
        this.metric = metric;
        this.tags = tags;
    }

    public Timeline(String metric, Map<String, String> tags, List<String> fields) {
        this.metric = metric;
        this.tags = tags;
        this.fields = fields;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}
