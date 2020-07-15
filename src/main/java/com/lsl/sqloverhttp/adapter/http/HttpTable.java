package com.lsl.sqloverhttp.adapter.http;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HttpTable extends AbstractTable implements ScannableTable {

    //具体执行扫表的例程
    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        try {
            getHttp();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new HttpRowEnumerator();
            }
        };
    }

    //搞定表里每列名字的例程
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        JavaTypeFactory typeFactory = (JavaTypeFactory)relDataTypeFactory;

        List<String> names = new ArrayList<>();
        names.add("id");
        names.add("name");
        List<RelDataType> types = new ArrayList<>();
        types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
        types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));

        return typeFactory.createStructType(Pair.zip(names,types));
    }

    private String getHttp() throws IOException {
        CloseableHttpResponse Response = null;
        CloseableHttpClient client = HttpClients.createDefault();    //创建一个http客户端
        HttpGet httpGet = new HttpGet("http://172.18.161.113:8888/"); // 通过httpget方式来实现我们的get请求
        try {
            //请求执行
            Response = client.execute(httpGet); // 通过client调用execute方法，得到我们的执行结果就是一个response，所有的数据都封装在response里面了
            if (Response.getStatusLine().getStatusCode() == 200) {
                String content = EntityUtils.toString(Response.getEntity(), "utf-8");
                System.out.println("Http respon--------->" + content);
                return content;
            }
            return "";
        } finally {
            if (Response != null) {
                Response.close();
            }
            Response.close();
            return "";
        }
    }
}
