package com.dtstack.dtcenter.common.loader.common.utils;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.dto.source.RdbmsSourceDTO;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * properties 工具类
 *
 * @author ：wangchuan
 * date：Created in 下午3:25 2022/1/21
 * company: www.dtstack.com
 */
public class PropertiesUtil {

    public static Properties convertToProp(RdbmsSourceDTO rdbmsSourceDTO) {
        return convertToProp(rdbmsSourceDTO, null);
    }

    public static Properties convertToProp(RdbmsSourceDTO rdbmsSourceDTO, Properties properties) {
        if (Objects.isNull(properties)) {
            properties = new Properties();
        }

        if (StringUtils.isNotBlank(rdbmsSourceDTO.getUsername())) {
            properties.setProperty("user", rdbmsSourceDTO.getUsername());
        }

        if (StringUtils.isNotBlank(rdbmsSourceDTO.getPassword())) {
            properties.setProperty("password", rdbmsSourceDTO.getPassword());
        }

        if (ReflectUtil.fieldExists(RdbmsSourceDTO.class, "properties") && StringUtils.isNotBlank(rdbmsSourceDTO.getProperties())) {
            JSONObject propertiesJson = JSONUtil.parseJsonObject(rdbmsSourceDTO.getProperties());
            for (String key : propertiesJson.keySet()) {
                String value = propertiesJson.getString(key);
                if (StringUtils.isNotBlank(value)) {
                    properties.setProperty(key, value);
                }
            }
        }

        return properties;
    }
}
