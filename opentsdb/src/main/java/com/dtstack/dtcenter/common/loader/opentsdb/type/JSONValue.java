package com.dtstack.dtcenter.common.loader.opentsdb.type;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

public class JSONValue {

    public static <T extends JSONValue> T parseObject(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    public String toJSON() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect, SerializerFeature.SortField, SerializerFeature.SortField.MapSortField);
    }

    @Override
    public String toString() {
        return toJSON();
    }

    public void appendJSON(final StringBuilder sb) {
        sb.append(toJSON());
    }

}
