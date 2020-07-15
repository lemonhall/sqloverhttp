import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.*;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class Hello {
    protected static final URL url = Hello.class.getResource("/http.model.json");


    public static void main(String[] args) throws ClientProtocolException, IOException, SQLException {
//        CloseableHttpResponse Response = null;
//        CloseableHttpClient client = HttpClients.createDefault();    //创建一个http客户端
//        HttpGet httpGet = new HttpGet("http://172.29.130.193:8888/"); // 通过httpget方式来实现我们的get请求
//        try {
//            //请求执行
//            Response = client.execute(httpGet); // 通过client调用execute方法，得到我们的执行结果就是一个response，所有的数据都封装在response里面了
//            if (Response.getStatusLine().getStatusCode() == 200) {
//                String content = EntityUtils.toString(Response.getEntity(), "utf-8");
//                System.out.println("--------->" + content);
//            }
//        } finally {
//            if (Response != null) {
//                Response.close();
//            }
//            Response.close();
//        }
        String str = URLDecoder.decode(url.toString(), "UTF-8");
        Properties info = new Properties();
        info.put("model", str.replace("file:", ""));

        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from TT");
            while (resultSet.next()) {
                System.out.println("data => ");
                System.out.println(resultSet.getObject("id"));
                System.out.println(resultSet.getObject("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }
}
