package com.lsl.sqloverhttp.adapter.http;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;


public class HttpSchema extends AbstractSchema {
    private Map<String, Table> tableMap;

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            HttpTable t = new HttpTable();
                  builder.put("TT",t);    // 一个数据库有多个表名，这里初始化，大小写要注意了,TEST01是表名。
            tableMap = builder.build();
        }
        return tableMap;
    }
}
