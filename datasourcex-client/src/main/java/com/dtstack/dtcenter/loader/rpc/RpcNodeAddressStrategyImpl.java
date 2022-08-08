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

package com.dtstack.dtcenter.loader.rpc;

import com.dtstack.dtcenter.loader.AutoConfig;
import com.dtstack.dtcenter.loader.constant.CommonConstant;
import com.dtstack.dtcenter.loader.exception.DtLoaderException;
import com.dtstack.rpc.enums.RpcRemoteType;
import com.dtstack.rpc.exception.ExecuteException;
import com.dtstack.rpc.manager.RpcNodeAddressStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * rpc node address impl
 *
 * @author ：wangchuan
 * date：Created in 上午11:34 2022/3/22
 * company: www.dtstack.com
 */
@Slf4j
@Service
public class RpcNodeAddressStrategyImpl implements RpcNodeAddressStrategy {

    @Override
    public RpcRemoteType getRpcRemoteType() {
        return RpcRemoteType.DATASOURCEX_CLIENT;
    }

    @Override
    public Set<String> getAllNodes(Object[] params) throws ExecuteException {
        String serverNodes = AutoConfig.getEnvWithThrow(CommonConstant.SERVER_NODES);
        log.debug("server nodes is: {}", serverNodes);
        return Arrays.stream(serverNodes.split(",")).collect(Collectors.toSet());
    }
}
