package com.lsl.sqloverhttp.adapter.http;


import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;

import java.util.List;
import java.util.Map;

@SuppressWarnings("UnusedDeclaration")
public class HttpSchemaFactory implements SchemaFactory {
    @Override
    public Schema create(SchemaPlus schemaPlus, String sname, Map<String, Object> operand) {
        //检查是否有tables参数
        Preconditions.checkArgument(operand.get("tables") != null,
                "tables must be specified");
        //从json里取出tables参数
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tables = (List) operand.get("tables");
        //将所有的http接口（表）的参数传递给下一层
        return new HttpSchema(tables);
    }
}
