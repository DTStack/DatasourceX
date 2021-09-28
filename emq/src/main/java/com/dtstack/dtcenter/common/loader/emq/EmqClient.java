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

package com.dtstack.dtcenter.common.loader.emq;

import com.dtstack.dtcenter.common.loader.common.nosql.AbsNoSqlClient;
import com.dtstack.dtcenter.loader.dto.source.EMQSourceDTO;
import com.dtstack.dtcenter.loader.dto.source.ISourceDTO;

/**
 * Date: 2020/4/7
 * Company: www.dtstack.com
 *
 * @author xiaochen
 */
public class EmqClient<T> extends AbsNoSqlClient<T> {
    @Override
    public Boolean testCon(ISourceDTO iSource) {
        EMQSourceDTO emqSourceDTO = (EMQSourceDTO) iSource;
        return EMQUtils.checkConnection(emqSourceDTO.getUrl(), emqSourceDTO.getUsername(), emqSourceDTO.getPassword());
    }
}
