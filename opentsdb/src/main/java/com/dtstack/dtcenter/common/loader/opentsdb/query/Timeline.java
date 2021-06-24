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
