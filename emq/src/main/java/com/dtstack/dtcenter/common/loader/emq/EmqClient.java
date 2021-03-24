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
