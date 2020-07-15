package com.lsl.sqloverhttp.adapter.http;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.*;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


public class HttpSchema extends AbstractSchema {
    private Map<String, Table> tableMap;
    //这部分都是直接从redis的Adapter抄过来的
    public final List<Map<String, Object>> tables;


    public HttpSchema(List<Map<String, Object>> tables) {
        this.tables = tables;
    }

//    @Override
//    protected Map<String, Table> getTableMap() {
//        if (tableMap == null) {
//            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
//            HttpTable t = new HttpTable();
//                  builder.put("TT",t);    // 一个数据库有多个表名，这里初始化，大小写要注意了,TEST01是表名。
//            tableMap = builder.build();
//        }
//        return tableMap;
//    }

    //直接从redisAapter那边拷贝过来的一个方法
    //核心还是构造一个Map出来，一个以表名：Table对象为键值对的Map
    @Override protected Map<String, Table> getTableMap() {
        JsonCustomTable[] jsonCustomTables = new JsonCustomTable[tables.size()];
        Set<String> tableNames = Arrays.stream(tables.toArray(jsonCustomTables))
                .map(e -> e.name).collect(Collectors.toSet());
        tableMap = Maps.asMap(
                ImmutableSet.copyOf(tableNames),
                CacheBuilder.newBuilder()
                        .build(CacheLoader.from(this::table)));//这里调用了本地的table方法
        return tableMap;
    }

    //实际上就要开始在这里搞事情了呢；
    private HttpTable table(String tableName) {
        //抄redis那边
        return null;
    }
}
