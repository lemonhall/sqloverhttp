package com.lsl.sqloverhttp.adapter.http;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class HttpTableFactory implements TableFactory {
    @Override
    public Table create(SchemaPlus schemaPlus, String tableName, Map operand, RelDataType relDataType) {
        //从上层解算出自己的Schema的参数
        final HttpSchema httpSchema = schemaPlus.unwrap(HttpSchema.class);
        //抄redis那边
        String url = operand.get("url") == null ? null
                : operand.get("url").toString();
        System.out.println("tableName ==>"+tableName+"\nurl => "+url);
        return new HttpTable();
    }
}
