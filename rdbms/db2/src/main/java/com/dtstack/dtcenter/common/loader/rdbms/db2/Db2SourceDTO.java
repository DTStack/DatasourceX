package com.dtstack.dtcenter.common.loader.rdbms.db2;

import com.dtstack.dtcenter.loader.dto.ISourceDTO;

import java.sql.Connection;

public class Db2SourceDTO implements ISourceDTO {

    @Override
    public Connection clearAfterGetConnection(Integer clearStatus) {
        return null;
    }
}
