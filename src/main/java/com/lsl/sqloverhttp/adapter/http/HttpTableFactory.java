package com.lsl.sqloverhttp.adapter.http;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;

import java.util.Map;

public class HttpTableFactory implements TableFactory {
    @Override
    public Table create(SchemaPlus schemaPlus, String name, Map map, RelDataType relDataType) {
        return new HttpTable();
    }
}
