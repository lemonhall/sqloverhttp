package com.lsl.sqloverhttp.adapter.http;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.util.Set;


public class HttpTable extends AbstractTable implements ScannableTable {
    final String tableName;
    final Set<String> colKeySet;
    final JsonArray dataArray;

    public HttpTable(String tableName, Set<String> colKeySet, JsonArray dataArray) {
        this.tableName = tableName;
        this.colKeySet = colKeySet;
        this.dataArray = dataArray;
    }


    //拿到表所需要的url
    static HttpTable create(String tableName,String url) {

        Set<String> colKeySet = null;
        JsonArray dataArray = null;

        try {
            String json =getHttp(url);
            // json解析器，解析json数据
            JsonElement element = JsonParser.parseString(json);
            //保证拿到的数据是一个json的array
            if (element.isJsonArray()) {
                //将元素转换为array
                dataArray = element.getAsJsonArray();
                //取行的第一个元素即可
                JsonObject item = dataArray.get(0).getAsJsonObject();
                //渠道第一行这个元素里的列明赋给变量colkeyset
                colKeySet = item.keySet();
                for (String str : colKeySet) {
                    System.out.println(str);
                }
            }else {
                System.out.println("Response is not an json array");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //传入列名和数据对象
        return new HttpTable(tableName,colKeySet,dataArray);
    }

    //具体执行扫表的例程
    @Override
    public Enumerable<Object[]> scan(DataContext dataContext) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new HttpRowEnumerator(dataArray);
            }
        };
    }

    //搞定表里每列名字的例程
    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        JavaTypeFactory typeFactory = (JavaTypeFactory)relDataTypeFactory;
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();

        for (String str : colKeySet) {
            names.add(str);
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
            System.out.println("reg col name to:"+tableName+":"+str);
        }
        return typeFactory.createStructType(Pair.zip(names,types));
    }

    private static String getHttp(String url) throws IOException {
        CloseableHttpResponse Response = null;
        CloseableHttpClient client = HttpClients.createDefault();    //创建一个http客户端
        String content = null;

        HttpGet httpGet = new HttpGet(url); // 通过httpget方式来实现我们的get请求
        try {
            //请求执行
            Response = client.execute(httpGet); // 通过client调用execute方法，得到我们的执行结果就是一个response，所有的数据都封装在response里面了
            if (Response.getStatusLine().getStatusCode() == 200) {
                content = EntityUtils.toString(Response.getEntity(), "utf-8");
                System.out.println("Http respon--------->" + content);
                return content;
            }
            return "";
        } finally {
            if (Response != null) {
                Response.close();
                return content;
            }
            Response.close();
            return "";
        }
    }
}
