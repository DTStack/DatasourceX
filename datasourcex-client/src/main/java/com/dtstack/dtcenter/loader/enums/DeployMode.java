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

package com.dtstack.dtcenter.loader.enums;

import org.apache.commons.lang3.StringUtils;

/**
 * 部署方式, 现支持本地部署、client-server 部署方式
 *
 * @author ：wangchuan
 * date：Created in 上午3:01 2022/2/21
 * company: www.dtstack.com
 */
public enum DeployMode {

    /**
     * 本地部署
     */
    LOCAL("local"),

    /**
     * 远程调用方式部署
     */
    REMOTE("remote");

    private final String type;

    DeployMode(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    /**
     * 获取部署模式, 默认 local
     *
     * @param type 部署方式
     * @return 部署模式枚举
     */
    public static DeployMode getDeployMode(String type) {
        for (DeployMode mode : values()) {
            if (StringUtils.equalsIgnoreCase(mode.getType(), type)) {
                return mode;
            }
        }
        return LOCAL;
    }
}
