package com.dtstack.dtcenter.loader.dto.source;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.dtcenter.loader.source.DataSourceType;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang3.StringUtils;

@Data
@ToString
@SuperBuilder
public class TbdsHdfsSourceDTO extends HdfsSourceDTO {

    private static final String TBDS_AUTH = "hadoop.security.authentication";

    private static final String TBDS_NAME = "hadoop.security.authentication.tbds_username";

    private static final String TBDS_ID = "hadoop.security.authentication.tbds.secureid";

    private static final String TBDS_KEY = "hadoop.security.authentication.tbds.securekey";

    private String tbdsUsername;

    private String tbdsSecureId;

    private String tbdsSecureKey;

    @Override
    public Integer getSourceType() {
        return DataSourceType.TBDS_HDFS.getVal();
    }

    @Override
    public String getConfig() {
        String config = super.getConfig();
        JSONObject configJson;
        if (StringUtils.isNotEmpty(config)) {
            configJson = JSONObject.parseObject(config);
        } else {
            configJson = new JSONObject();
        }
        if (StringUtils.isNotEmpty(tbdsUsername)) {
            configJson.put(TBDS_NAME, tbdsUsername);
        }
        if (StringUtils.isNotEmpty(tbdsSecureId)) {
            configJson.put(TBDS_ID, tbdsSecureId);
        }
        if (StringUtils.isNotEmpty(tbdsSecureKey)) {
            configJson.put(TBDS_KEY, tbdsSecureKey);
        }

        if (StringUtils.isNotEmpty(configJson.getString(TBDS_NAME)) ||
                StringUtils.isNotEmpty(configJson.getString(TBDS_ID)) ||
                StringUtils.isNotEmpty(configJson.getString(TBDS_KEY))) {
            configJson.put(TBDS_AUTH, "tbds");
        }
        return configJson.toJSONString();
    }
}
