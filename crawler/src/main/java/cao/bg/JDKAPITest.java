package cao.bg;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
    演示使用JDK自带的API实现网络爬虫
 */

public class JDKAPITest {
    @Test
    public void testGet() throws Exception {
        //1.确定要访问/爬取的URL
        URL url = new URL("https://www.bilibili.com/?username=xx");

        //2.获取连接对象
        HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();

        //3.设置连接信息:请求方式/请求参数/请求头...
        urlConnection.setRequestMethod("GET");//请求方式默认就是GET,注意要大写
        urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36");
        urlConnection.setConnectTimeout(30000); //设置超时时间,单位毫秒

        //4.获取数据
        InputStream in = urlConnection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        String html ="";
        while ((line = reader.readLine()) !=null){
            html += line +"\n";
        }
        System.out.println(html);

        //5.关闭资源
        in.close();
        reader.close();

    }

    @Test
    public void testPost() throws Exception {
        //1.确定URL
        URL url = new URL("https://www.bilibili.com/");

        //2.获取连接
        HttpURLConnection urlConnection = (HttpURLConnection)url.openConnection();

        //3.设置连接信息
        urlConnection.setDoOutput(true);//允许向url输出内容
        urlConnection.setRequestMethod("POST");//请求方式默认就是GET,注意要大写
        urlConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36");
        urlConnection.setConnectTimeout(30000); //设置超时时间,单位毫秒
        OutputStream out = urlConnection.getOutputStream();
        out.write("username=xx".getBytes());

        //4.获取数据
        InputStream in = urlConnection.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line;
        String html ="";
        while ((line = reader.readLine()) !=null){
            html += line +"\n";
        }
        System.out.println(html);

        //5.关闭资源
        in.close();
        reader.close();
    }

}
